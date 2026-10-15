from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.enums import EventType
from app.schemas.events import parse_payload


def test_run_created_payload_valid() -> None:
    parsed = parse_payload(
        EventType.RUN_CREATED, {"workflow_name": "report_workflow", "input": {"task": "x"}}
    )
    assert parsed.workflow_name == "report_workflow"


def test_tool_requested_requires_idempotency_key() -> None:
    with pytest.raises(ValidationError):
        parse_payload(
            EventType.TOOL_CALL_REQUESTED,
            {"step_id": "save_report", "tool_name": "save_report", "arguments": {}},
        )


def test_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        parse_payload(EventType.RUN_STARTED, {"worker_id": "w1", "extra": True})
