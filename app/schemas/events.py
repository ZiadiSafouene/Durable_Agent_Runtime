from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.models.enums import EventType


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RunCreatedPayload(StrictModel):
    workflow_name: str
    input: dict[str, Any]


class RunStartedPayload(StrictModel):
    worker_id: str


class StepStartedPayload(StrictModel):
    step_id: str
    step_name: str
    input: dict[str, Any] = Field(default_factory=dict)


class StepCompletedPayload(StrictModel):
    step_id: str
    output: dict[str, Any]


class ToolCallRequestedPayload(StrictModel):
    step_id: str
    tool_name: str
    idempotency_key: str
    arguments: dict[str, Any]


class ToolCallSucceededPayload(StrictModel):
    step_id: str
    tool_name: str
    idempotency_key: str
    result: dict[str, Any]


class ToolCallFailedPayload(StrictModel):
    step_id: str
    tool_name: str
    idempotency_key: str
    error: str
    retryable: bool


class LlmCallRecordedPayload(StrictModel):
    step_id: str
    provider: str
    model: str
    prompt_ref: str
    request_params: dict[str, Any]
    response: dict[str, Any]


class RunFailedPayload(StrictModel):
    error: str
    retryable: bool
    step_id: str | None = None


class RunCompletedPayload(StrictModel):
    result: dict[str, Any]


class RunCancelledPayload(StrictModel):
    reason: str


class RunRetryRequestedPayload(StrictModel):
    reason: str


EVENT_PAYLOAD_TYPES: dict[EventType, type[BaseModel]] = {
    EventType.RUN_CREATED: RunCreatedPayload,
    EventType.RUN_STARTED: RunStartedPayload,
    EventType.STEP_STARTED: StepStartedPayload,
    EventType.STEP_COMPLETED: StepCompletedPayload,
    EventType.TOOL_CALL_REQUESTED: ToolCallRequestedPayload,
    EventType.TOOL_CALL_SUCCEEDED: ToolCallSucceededPayload,
    EventType.TOOL_CALL_FAILED: ToolCallFailedPayload,
    EventType.LLM_CALL_RECORDED: LlmCallRecordedPayload,
    EventType.RUN_FAILED: RunFailedPayload,
    EventType.RUN_COMPLETED: RunCompletedPayload,
    EventType.RUN_CANCELLED: RunCancelledPayload,
    EventType.RUN_RETRY_REQUESTED: RunRetryRequestedPayload,
}


class NewEvent(BaseModel):
    event_type: EventType
    payload: dict[str, Any]
    schema_version: int = 1
    correlation_id: str | None = None
    causation_id: str | None = None


def parse_payload(event_type: EventType, payload: dict[str, Any]) -> BaseModel:
    model = EVENT_PAYLOAD_TYPES[event_type]
    return model.model_validate(payload)
