from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.enums import EventType, OperationStatus, RunStatus
from app.models.tables import WorkflowEvent
from app.observability import log_extra
from app.runtime.faults import FaultController
from app.runtime.idempotency import llm_prompt_ref, tool_idempotency_key
from app.runtime.llm import FakeLlmProvider, LlmProvider
from app.runtime.retry import PermanentError, RetryPolicy, RetryableError
from app.runtime.state import WorkflowState, reduce_events
from app.runtime.workflow import (
    REPORT_WORKFLOW,
    StepDef,
    analysis_from_llm,
    build_report_body,
    compute_report,
    get_workflow,
    next_step,
)
from app.schemas.events import NewEvent
from app.storage.event_store import EventStore
from app.storage.repositories import RunRepository, SnapshotRepository, ToolRepository
from app.tools.executor import ToolExecutor

logger = logging.getLogger(__name__)


class LostLeaseError(RuntimeError):
    pass


class ReplayError(RuntimeError):
    pass


@dataclass
class Engine:
    session_factory: async_sessionmaker[AsyncSession]
    tool_executor: ToolExecutor
    llm: LlmProvider
    policy: RetryPolicy
    snapshot_every: int = 50
    faults: FaultController | None = None
    mode: Literal["execute", "replay"] = "execute"

    async def _hit(self, point: str) -> None:
        if self.faults is not None:
            await self.faults.hit(point)

    async def reconstruct(self, session: AsyncSession, run_id: str) -> WorkflowState:
        snapshots = SnapshotRepository(session)
        store = EventStore(session)
        snap = await snapshots.latest(run_id)
        if snap is not None:
            state = WorkflowState.from_payload(snap.state_payload)
            events = await store.list_events(run_id, after_sequence=snap.sequence)
            return reduce_events(run_id, events, initial=state)
        events = await store.list_events(run_id)
        return reduce_events(run_id, events)

    async def reconstruct_full(self, session: AsyncSession, run_id: str) -> WorkflowState:
        events = await EventStore(session).list_events(run_id)
        return reduce_events(run_id, events)

    async def process_run(self, run_id: str, worker_id: str) -> WorkflowState:
        while True:
            async with self.session_factory() as session:
                runs = RunRepository(session)
                run = await runs.get(run_id)
                if run is None:
                    raise RuntimeError(f"run {run_id} not found")
                state = await self.reconstruct(session, run_id)
                if state.status in {
                    RunStatus.COMPLETED.value,
                    RunStatus.FAILED.value,
                    RunStatus.CANCELLED.value,
                }:
                    if self.mode == "execute" and run.lease_owner == worker_id:
                        await runs.release(run)
                        await session.commit()
                    return state
                if self.mode == "execute" and not await runs.owns_lease(run_id, worker_id):
                    raise LostLeaseError(f"worker {worker_id} lost lease on {run_id}")
                action = self._decide(state)
                if action is None:
                    await session.commit()
                    return state

            delay = await self._apply(run_id, worker_id, action)
            if delay:
                await asyncio.sleep(delay)
            await self._hit("after_step_complete_before_next")

    def _decide(self, state: WorkflowState) -> dict[str, Any] | None:
        if state.status == RunStatus.PENDING.value:
            return {"kind": "start"}
        if state.status != RunStatus.RUNNING.value:
            return None
        if state.current_step_id is not None:
            step = _step_by_id(state.workflow_name, state.current_step_id)
            if step.kind == "llm" and state.current_step_id not in state.llm_outputs:
                return {"kind": "llm", "step": step}
            if step.kind == "llm":
                return {"kind": "complete_llm_step", "step": step}
            if step.kind == "compute":
                return {"kind": "compute", "step": step}
            if step.kind == "tool":
                key = tool_idempotency_key(state.run_id, step.id, step.tool_name or "")
                recorded = state.tools.get(key)
                if state.pending_tool is not None:
                    return {"kind": "run_tool", "view": state.pending_tool}
                if recorded and recorded.status == "succeeded":
                    return {"kind": "complete_tool_step", "step": step, "view": recorded}
                if recorded and recorded.status in {"requested", "failed"}:
                    return {"kind": "run_tool", "view": recorded}
                return {"kind": "request_tool", "step": step}
            if step.kind == "finalize":
                return {"kind": "finalize", "step": step}
        nxt = next_step(state.completed_step_ids, get_workflow(state.workflow_name or REPORT_WORKFLOW))
        if nxt is None:
            return {"kind": "complete_run"}
        return {"kind": "start_step", "step": nxt}

    async def _apply(self, run_id: str, worker_id: str, action: dict[str, Any]) -> float:
        kind = action["kind"]
        if kind == "start":
            await self._append(
                run_id,
                worker_id,
                [NewEvent(event_type=EventType.RUN_STARTED, payload={"worker_id": worker_id})],
            )
            return 0.0
        if kind == "start_step":
            step: StepDef = action["step"]
            await self._append(
                run_id,
                worker_id,
                [
                    NewEvent(
                        event_type=EventType.STEP_STARTED,
                        payload={"step_id": step.id, "step_name": step.name, "input": {}},
                    )
                ],
            )
            return 0.0
        if kind == "llm":
            await self._run_llm(run_id, worker_id, action["step"])
            return 0.0
        if kind == "complete_llm_step":
            step = action["step"]
            async with self.session_factory() as session:
                state = await self.reconstruct(session, run_id)
                recorded = state.llm_outputs[step.id]
                output = analysis_from_llm(recorded)
                await self._append_in(
                    session,
                    run_id,
                    worker_id,
                    [NewEvent(event_type=EventType.STEP_COMPLETED, payload={"step_id": step.id, "output": output})],
                )
                await session.commit()
            return 0.0
        if kind == "compute":
            await self._compute(run_id, worker_id, action["step"])
            return 0.0
        if kind == "request_tool":
            await self._request_tool(run_id, worker_id, action["step"])
            return 0.0
        if kind == "run_tool":
            return await self._run_tool(run_id, worker_id, action["view"])
        if kind == "complete_tool_step":
            step = action["step"]
            view = action["view"]
            async with self.session_factory() as session:
                await self._append_in(
                    session,
                    run_id,
                    worker_id,
                    [
                        NewEvent(
                            event_type=EventType.STEP_COMPLETED,
                            payload={"step_id": step.id, "output": view.result or {}},
                        )
                    ],
                )
                await session.commit()
            return 0.0
        if kind == "finalize":
            await self._finalize(run_id, worker_id, action["step"])
            return 0.0
        if kind == "complete_run":
            async with self.session_factory() as session:
                state = await self.reconstruct(session, run_id)
            result = state.step_outputs.get("finalize") or state.result or {}
            await self._complete(run_id, worker_id, result)
            return 0.0
        raise RuntimeError(f"unknown action {kind}")

    async def _run_llm(self, run_id: str, worker_id: str, step: StepDef) -> None:
        async with self.session_factory() as session:
            state = await self.reconstruct(session, run_id)
            task = str(state.input.get("task", ""))
            prompt = f"Analyze the following task for a short report plan.\nTASK:\n{task}"
            if step.id in state.llm_outputs:
                recorded = state.llm_outputs[step.id]
                output = analysis_from_llm(recorded)
                events = [
                    NewEvent(event_type=EventType.STEP_COMPLETED, payload={"step_id": step.id, "output": output})
                ]
                await self._append_in(session, run_id, worker_id, events)
                await session.commit()
                return
            if self.mode == "replay":
                raise ReplayError(f"missing recorded LLM output for step {step.id}")
            await self._hit("before_llm")
            response = await self.llm.complete(prompt, purpose="analyze", params={"temperature": 0})
            recorded = {
                "step_id": step.id,
                "provider": response.provider,
                "model": response.model,
                "prompt_ref": llm_prompt_ref(run_id, step.id, prompt),
                "request_params": {"temperature": 0, "purpose": "analyze"},
                "response": {"text": response.text, "usage": response.usage},
            }
            output = analysis_from_llm(recorded)
            events = [
                NewEvent(event_type=EventType.LLM_CALL_RECORDED, payload=recorded),
                NewEvent(event_type=EventType.STEP_COMPLETED, payload={"step_id": step.id, "output": output}),
            ]
            await self._append_in(session, run_id, worker_id, events)
            await session.commit()

    async def _compute(self, run_id: str, worker_id: str, step: StepDef) -> None:
        async with self.session_factory() as session:
            state = await self.reconstruct(session, run_id)
            analysis = state.step_outputs.get("analyze") or {}
            output = compute_report(str(state.input.get("task", "")), analysis)
            await self._append_in(
                session,
                run_id,
                worker_id,
                [NewEvent(event_type=EventType.STEP_COMPLETED, payload={"step_id": step.id, "output": output})],
            )
            await session.commit()

    async def _request_tool(self, run_id: str, worker_id: str, step: StepDef) -> None:
        await self._hit("before_tool_begin")
        async with self.session_factory() as session:
            state = await self.reconstruct(session, run_id)
            arguments = self._tool_arguments(state, step)
            key = tool_idempotency_key(run_id, step.id, step.tool_name or "")
            await ToolRepository(session).ensure_requested(
                idempotency_key=key,
                run_id=run_id,
                step_id=step.id,
                tool_name=step.tool_name or "",
                request_payload=arguments,
            )
            await self._append_in(
                session,
                run_id,
                worker_id,
                [
                    NewEvent(
                        event_type=EventType.TOOL_CALL_REQUESTED,
                        payload={
                            "step_id": step.id,
                            "tool_name": step.tool_name,
                            "idempotency_key": key,
                            "arguments": arguments,
                        },
                    )
                ],
            )
            await session.commit()
        await self._hit("after_tool_requested_before_effect")

    def _tool_arguments(self, state: WorkflowState, step: StepDef) -> dict[str, Any]:
        task = str(state.input.get("task", ""))
        computed = state.step_outputs.get("compute") or {}
        if step.tool_name == "save_report":
            return {"title": computed.get("report_id") or "report", "body": build_report_body(task, computed)}
        if step.tool_name == "notify":
            saved = state.tools
            report_effect = None
            for view in saved.values():
                if view.tool_name == "save_report" and view.result:
                    report_effect = view.result.get("effect_id")
            return {
                "channel": state.input.get("notify_channel") or "email",
                "message": f"Report ready for '{task[:80]}' effect={report_effect}",
            }
        raise RuntimeError(f"no argument builder for {step.tool_name}")

    async def _run_tool(self, run_id: str, worker_id: str, view: Any) -> float:
        step_id = view.step_id
        tool_name = view.tool_name
        key = view.idempotency_key
        arguments = view.arguments
        if self.mode == "replay":
            async with self.session_factory() as session:
                op = await ToolRepository(session).get_by_key(key)
                if op is None or op.status != OperationStatus.SUCCEEDED.value:
                    if view.result:
                        result = view.result
                    else:
                        raise ReplayError(f"missing recorded tool result for {key}")
                else:
                    result = op.result_payload or {}
                await self._append_in(
                    session,
                    run_id,
                    worker_id,
                    [
                        NewEvent(
                            event_type=EventType.TOOL_CALL_SUCCEEDED,
                            payload={
                                "step_id": step_id,
                                "tool_name": tool_name,
                                "idempotency_key": key,
                                "result": result,
                            },
                        ),
                        NewEvent(
                            event_type=EventType.STEP_COMPLETED,
                            payload={"step_id": step_id, "output": result},
                        ),
                    ],
                )
                await session.commit()
            return 0.0

        try:
            result = await self.tool_executor.execute(
                run_id=run_id,
                step_id=step_id,
                tool_name=tool_name,
                idempotency_key=key,
                arguments=arguments,
            )
        except RetryableError as exc:
            await self._on_tool_failure(run_id, worker_id, view, str(exc), retryable=True)
            async with self.session_factory() as session:
                op = await ToolRepository(session).get_by_key(view.idempotency_key)
                attempts = int(op.attempt_count) if op is not None else 1
            return self.policy.delay_for_attempt(attempts)
        except PermanentError as exc:
            await self._on_tool_failure(run_id, worker_id, view, str(exc), retryable=False)
            return 0.0

        await self._hit("after_effect_before_success_event")
        async with self.session_factory() as session:
            await self._append_in(
                session,
                run_id,
                worker_id,
                [
                    NewEvent(
                        event_type=EventType.TOOL_CALL_SUCCEEDED,
                        payload={
                            "step_id": step_id,
                            "tool_name": tool_name,
                            "idempotency_key": key,
                            "result": result,
                        },
                    ),
                    NewEvent(
                        event_type=EventType.STEP_COMPLETED,
                        payload={"step_id": step_id, "output": result},
                    ),
                ],
            )
            await session.commit()
        return 0.0

    async def _on_tool_failure(
        self, run_id: str, worker_id: str, view: Any, error: str, *, retryable: bool
    ) -> None:
        async with self.session_factory() as session:
            runs = RunRepository(session)
            run = await runs.get_for_update(run_id)
            if run is None:
                raise RuntimeError("missing run")
            await self._append_in(
                session,
                run_id,
                worker_id,
                [
                    NewEvent(
                        event_type=EventType.TOOL_CALL_FAILED,
                        payload={
                            "step_id": view.step_id,
                            "tool_name": view.tool_name,
                            "idempotency_key": view.idempotency_key,
                            "error": error,
                            "retryable": retryable,
                        },
                    )
                ],
            )
            attempts = 0
            op = await ToolRepository(session).get_by_key(view.idempotency_key)
            if op is not None:
                attempts = int(op.attempt_count)
            if not self.policy.should_retry(attempts, retryable):
                await self._append_in(
                    session,
                    run_id,
                    worker_id,
                    [
                        NewEvent(
                            event_type=EventType.RUN_FAILED,
                            payload={"error": error, "retryable": retryable, "step_id": view.step_id},
                        )
                    ],
                )
                run.status = RunStatus.FAILED.value
                run.error = error
                await runs.release(run)
            await session.commit()

    async def _finalize(self, run_id: str, worker_id: str, step: StepDef) -> None:
        async with self.session_factory() as session:
            state = await self.reconstruct(session, run_id)
            computed = state.step_outputs.get("compute") or {}
            save_out = state.step_outputs.get("save_report") or {}
            notify_out = state.step_outputs.get("notify") or {}
            result = {
                "report_id": computed.get("report_id"),
                "score": computed.get("score"),
                "save": save_out,
                "notify": notify_out,
                "summary": (state.step_outputs.get("analyze") or {}).get("summary"),
            }
            await self._append_in(
                session,
                run_id,
                worker_id,
                [
                    NewEvent(event_type=EventType.STEP_COMPLETED, payload={"step_id": step.id, "output": result}),
                    NewEvent(event_type=EventType.RUN_COMPLETED, payload={"result": result}),
                ],
            )
            run = await RunRepository(session).get_for_update(run_id)
            if run is not None:
                run.status = RunStatus.COMPLETED.value
                run.result_payload = result
                run.error = None
                await RunRepository(session).release(run)
            await session.commit()

    async def _complete(self, run_id: str, worker_id: str, result: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            await self._append_in(
                session,
                run_id,
                worker_id,
                [NewEvent(event_type=EventType.RUN_COMPLETED, payload={"result": result})],
            )
            run = await RunRepository(session).get_for_update(run_id)
            if run is not None:
                run.status = RunStatus.COMPLETED.value
                run.result_payload = result
                await RunRepository(session).release(run)
            await session.commit()

    async def _append(self, run_id: str, worker_id: str, events: list[NewEvent]) -> list[WorkflowEvent]:
        async with self.session_factory() as session:
            stored = await self._append_in(session, run_id, worker_id, events)
            await session.commit()
            return stored

    async def _append_in(
        self, session: AsyncSession, run_id: str, worker_id: str, events: list[NewEvent]
    ) -> list[WorkflowEvent]:
        runs = RunRepository(session)
        if self.mode == "execute" and not await runs.owns_lease(run_id, worker_id):
            raise LostLeaseError(f"worker {worker_id} lost lease on {run_id}")
        stored = await EventStore(session).append(run_id, events)
        state = await self.reconstruct(session, run_id)
        if self.snapshot_every > 0 and state.last_sequence > 0 and state.last_sequence % self.snapshot_every == 0:
            await SnapshotRepository(session).save(run_id, state.last_sequence, state.to_payload())
        logger.info(
            "events_appended",
            **log_extra(run_id=run_id, worker_id=worker_id, event_type=events[-1].event_type.value),
        )
        return stored


def _step_by_id(workflow_name: str, step_id: str) -> StepDef:
    for step in get_workflow(workflow_name or REPORT_WORKFLOW):
        if step.id == step_id:
            return step
    raise RuntimeError(f"unknown step {step_id}")


def default_engine(
    session_factory: async_sessionmaker[AsyncSession],
    tool_executor: ToolExecutor,
    *,
    llm: LlmProvider | None = None,
    faults: FaultController | None = None,
    policy: RetryPolicy | None = None,
    snapshot_every: int = 50,
    mode: Literal["execute", "replay"] = "execute",
) -> Engine:
    return Engine(
        session_factory=session_factory,
        tool_executor=tool_executor,
        llm=llm or FakeLlmProvider(),
        policy=policy or RetryPolicy(),
        snapshot_every=snapshot_every,
        faults=faults,
        mode=mode,
    )
