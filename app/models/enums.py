from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OperationStatus(StrEnum):
    REQUESTED = "requested"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    OUTCOME_UNKNOWN = "outcome_unknown"


class EventType(StrEnum):
    RUN_CREATED = "RunCreated"
    RUN_STARTED = "RunStarted"
    STEP_STARTED = "StepStarted"
    STEP_COMPLETED = "StepCompleted"
    TOOL_CALL_REQUESTED = "ToolCallRequested"
    TOOL_CALL_SUCCEEDED = "ToolCallSucceeded"
    TOOL_CALL_FAILED = "ToolCallFailed"
    LLM_CALL_RECORDED = "LlmCallRecorded"
    RUN_FAILED = "RunFailed"
    RUN_COMPLETED = "RunCompleted"
    RUN_CANCELLED = "RunCancelled"
    RUN_RETRY_REQUESTED = "RunRetryRequested"
