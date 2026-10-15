from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.runtime.state import WorkflowState, reduce_events
from app.runtime.workflow import get_workflow
from app.schemas.api import ReplayDecision, ReplayResponse
from app.storage.event_store import EventStore
from app.storage.repositories import SnapshotRepository, ToolRepository


async def reconstruct_state(session: AsyncSession, run_id: str) -> WorkflowState:
    snap = await SnapshotRepository(session).latest(run_id)
    store = EventStore(session)
    if snap is not None:
        state = WorkflowState.from_payload(snap.state_payload)
        events = await store.list_events(run_id, after_sequence=snap.sequence)
        return reduce_events(run_id, events, initial=state)
    events = await store.list_events(run_id)
    return reduce_events(run_id, events)


async def replay_recorded(session: AsyncSession, run_id: str) -> ReplayResponse:
    """Reproduce recorded decisions without executing tools or calling the LLM."""
    state = await reconstruct_state(session, run_id)
    events = await EventStore(session).list_events(run_id)
    notes: list[str] = [
        "Replay reconstructs decisions from the immutable event log.",
        "Recorded LLM payloads are reused; no new model calls are made.",
        "Successful tool results are reused; mock side effects are not executed.",
        "Replay does not make arbitrary external environments deterministic.",
    ]
    decisions: list[ReplayDecision] = []
    llm_reused = 0
    tools_reused = 0
    workflow = get_workflow(state.workflow_name or "report_workflow")
    for step in workflow:
        if step.id not in state.completed_step_ids and state.current_step_id != step.id:
            continue
        if step.kind == "llm":
            recorded = state.llm_outputs.get(step.id)
            decisions.append(
                ReplayDecision(
                    step_id=step.id,
                    action="reuse_llm",
                    reused_llm=recorded is not None,
                    detail={"present": recorded is not None},
                )
            )
            if recorded is not None:
                llm_reused += 1
            else:
                notes.append(f"No recorded LLM output for step {step.id}.")
        elif step.kind == "compute":
            decisions.append(
                ReplayDecision(
                    step_id=step.id,
                    action="recompute_deterministic",
                    detail={"output": state.step_outputs.get(step.id)},
                )
            )
        elif step.kind == "tool":
            tool_view = None
            for view in state.tools.values():
                if view.step_id == step.id:
                    tool_view = view
                    break
            op = None
            if tool_view is not None:
                op = await ToolRepository(session).get_by_key(tool_view.idempotency_key)
            reused = bool(
                (tool_view and tool_view.status == "succeeded")
                or (op is not None and op.status == "succeeded")
            )
            if reused:
                tools_reused += 1
            decisions.append(
                ReplayDecision(
                    step_id=step.id,
                    action="reuse_tool_result",
                    reused_tool_result=reused,
                    side_effects_executed=False,
                    detail={
                        "idempotency_key": tool_view.idempotency_key if tool_view else None,
                        "operation_status": op.status if op is not None else None,
                    },
                )
            )
        elif step.kind == "finalize":
            decisions.append(
                ReplayDecision(
                    step_id=step.id,
                    action="reuse_final_result",
                    detail={"result": state.result or state.step_outputs.get(step.id)},
                )
            )
    event_types = [event.event_type for event in events]
    notes.append(f"Event types in history: {event_types}")
    return ReplayResponse(
        run_id=run_id,
        reconstructed_status=state.status,
        last_sequence=state.last_sequence,
        decisions=decisions,
        llm_calls_reused=llm_reused,
        tool_results_reused=tools_reused,
        side_effects_executed=0,
        notes=notes,
    )
