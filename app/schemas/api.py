from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CreateRunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=4000)
    notify_channel: str = Field(default="email", min_length=1, max_length=64)
    workflow_name: str = Field(default="report_workflow", min_length=1, max_length=128)


class RunSummary(BaseModel):
    run_id: str
    status: str
    workflow_name: str
    created_at: datetime


class RunResponse(BaseModel):
    run_id: str
    status: str
    workflow_name: str
    input: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None
    current_step: str | None = None
    completed_steps: list[str] = Field(default_factory=list)
    last_sequence: int = 0
    attempt_count: int = 0
    created_at: datetime
    updated_at: datetime


class EventResponse(BaseModel):
    id: str
    run_id: str
    event_type: str
    sequence: int
    payload: dict[str, Any]
    schema_version: int
    correlation_id: str | None
    causation_id: str | None
    created_at: datetime


class ReplayDecision(BaseModel):
    step_id: str
    action: str
    reused_llm: bool = False
    reused_tool_result: bool = False
    side_effects_executed: bool = False
    detail: dict[str, Any] = Field(default_factory=dict)


class ReplayResponse(BaseModel):
    run_id: str
    reconstructed_status: str
    last_sequence: int
    decisions: list[ReplayDecision]
    llm_calls_reused: int
    tool_results_reused: int
    side_effects_executed: int
    notes: list[str]


class HealthResponse(BaseModel):
    status: str
    database: str | None = None
