from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from app.models.enums import EventType, RunStatus
from app.models.tables import WorkflowEvent
from app.schemas.events import parse_payload


class ReducerError(ValueError):
    pass


@dataclass
class ToolCallView:
    step_id: str
    tool_name: str
    idempotency_key: str
    arguments: dict[str, Any]
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool | None = None


@dataclass
class WorkflowState:
    run_id: str
    status: str = RunStatus.PENDING.value
    workflow_name: str = ""
    input: dict[str, Any] = field(default_factory=dict)
    current_step_id: str | None = None
    completed_step_ids: list[str] = field(default_factory=list)
    step_outputs: dict[str, Any] = field(default_factory=dict)
    last_sequence: int = 0
    result: dict[str, Any] | None = None
    error: str | None = None
    pending_tool: ToolCallView | None = None
    tools: dict[str, ToolCallView] = field(default_factory=dict)
    llm_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_event_id: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> WorkflowState:
        tools = {
            key: ToolCallView(**value) if not isinstance(value, ToolCallView) else value
            for key, value in payload.get("tools", {}).items()
        }
        pending = payload.get("pending_tool")
        if pending is not None and not isinstance(pending, ToolCallView):
            pending = ToolCallView(**pending)
        data = dict(payload)
        data["tools"] = tools
        data["pending_tool"] = pending
        return cls(**data)


_TERMINAL = {RunStatus.COMPLETED.value, RunStatus.FAILED.value, RunStatus.CANCELLED.value}


def reduce_event(state: WorkflowState, event: WorkflowEvent) -> WorkflowState:
    event_type = EventType(event.event_type)
    payload = parse_payload(event_type, event.payload)
    data = payload.model_dump()
    if event.sequence != state.last_sequence + 1:
        raise ReducerError(
            f"event sequence {event.sequence} does not follow {state.last_sequence} for run {state.run_id}"
        )
    state.last_sequence = event.sequence
    state.last_event_id = event.id

    if event_type == EventType.RUN_CREATED:
        if state.status != RunStatus.PENDING.value and state.workflow_name:
            raise ReducerError("RunCreated is only valid as the first meaningful event")
        state.workflow_name = data["workflow_name"]
        state.input = data["input"]
        state.status = RunStatus.PENDING.value
        return state

    if event_type == EventType.RUN_STARTED:
        if state.status not in {RunStatus.PENDING.value, RunStatus.RUNNING.value}:
            raise ReducerError(f"RunStarted from {state.status}")
        state.status = RunStatus.RUNNING.value
        return state

    if event_type == EventType.STEP_STARTED:
        if state.status != RunStatus.RUNNING.value:
            raise ReducerError("StepStarted requires running status")
        state.current_step_id = data["step_id"]
        return state

    if event_type == EventType.LLM_CALL_RECORDED:
        state.llm_outputs[data["step_id"]] = data
        return state

    if event_type == EventType.TOOL_CALL_REQUESTED:
        view = ToolCallView(
            step_id=data["step_id"],
            tool_name=data["tool_name"],
            idempotency_key=data["idempotency_key"],
            arguments=data["arguments"],
            status="requested",
        )
        state.pending_tool = view
        state.tools[data["idempotency_key"]] = view
        return state

    if event_type == EventType.TOOL_CALL_SUCCEEDED:
        key = data["idempotency_key"]
        view = state.tools.get(key)
        if view is None:
            view = ToolCallView(
                step_id=data["step_id"],
                tool_name=data["tool_name"],
                idempotency_key=key,
                arguments={},
                status="succeeded",
                result=data["result"],
            )
        else:
            view.status = "succeeded"
            view.result = data["result"]
        state.tools[key] = view
        if state.pending_tool and state.pending_tool.idempotency_key == key:
            state.pending_tool = None
        return state

    if event_type == EventType.TOOL_CALL_FAILED:
        key = data["idempotency_key"]
        view = state.tools.get(key)
        if view is None:
            view = ToolCallView(
                step_id=data["step_id"],
                tool_name=data["tool_name"],
                idempotency_key=key,
                arguments={},
                status="failed",
                error=data["error"],
                retryable=data["retryable"],
            )
        else:
            view.status = "failed"
            view.error = data["error"]
            view.retryable = data["retryable"]
        state.tools[key] = view
        if state.pending_tool and state.pending_tool.idempotency_key == key:
            state.pending_tool = None
        return state

    if event_type == EventType.STEP_COMPLETED:
        step_id = data["step_id"]
        if step_id not in state.completed_step_ids:
            state.completed_step_ids.append(step_id)
        state.step_outputs[step_id] = data["output"]
        if state.current_step_id == step_id:
            state.current_step_id = None
        return state

    if event_type == EventType.RUN_COMPLETED:
        state.status = RunStatus.COMPLETED.value
        state.result = data["result"]
        state.error = None
        return state

    if event_type == EventType.RUN_FAILED:
        state.status = RunStatus.FAILED.value
        state.error = data["error"]
        return state

    if event_type == EventType.RUN_CANCELLED:
        state.status = RunStatus.CANCELLED.value
        state.error = data["reason"]
        return state

    if event_type == EventType.RUN_RETRY_REQUESTED:
        if state.status not in {RunStatus.FAILED.value, RunStatus.CANCELLED.value}:
            raise ReducerError("retry is only valid from failed or cancelled")
        state.status = RunStatus.PENDING.value
        state.error = None
        return state

    raise ReducerError(f"unsupported event type {event_type}")


def reduce_events(run_id: str, events: list[WorkflowEvent], *, initial: WorkflowState | None = None) -> WorkflowState:
    state = initial or WorkflowState(run_id=run_id)
    for event in events:
        state = reduce_event(state, event)
    return state


def is_terminal(status: str) -> bool:
    return status in _TERMINAL
