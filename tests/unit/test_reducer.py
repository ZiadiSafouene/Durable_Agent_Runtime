from __future__ import annotations

import pytest

from app.models.enums import EventType, RunStatus
from app.runtime.state import ReducerError, WorkflowState, reduce_event, reduce_events
from tests.helpers import make_event


def test_full_happy_path_transitions() -> None:
    run_id = "run-1"
    events = [
        make_event(
            run_id,
            EventType.RUN_CREATED,
            1,
            {"workflow_name": "report_workflow", "input": {"task": "hello"}},
        ),
        make_event(run_id, EventType.RUN_STARTED, 2, {"worker_id": "w"}),
        make_event(
            run_id, EventType.STEP_STARTED, 3, {"step_id": "analyze", "step_name": "Analyze", "input": {}}
        ),
        make_event(
            run_id,
            EventType.LLM_CALL_RECORDED,
            4,
            {
                "step_id": "analyze",
                "provider": "fake",
                "model": "fake-1",
                "prompt_ref": "r",
                "request_params": {},
                "response": {"text": "summary=ok; keywords=a"},
            },
        ),
        make_event(
            run_id, EventType.STEP_COMPLETED, 5, {"step_id": "analyze", "output": {"summary": "ok"}}
        ),
        make_event(run_id, EventType.RUN_COMPLETED, 6, {"result": {"ok": True}}),
    ]
    state = reduce_events(run_id, events)
    assert state.status == RunStatus.COMPLETED.value
    assert state.completed_step_ids == ["analyze"]
    assert state.result == {"ok": True}
    assert "analyze" in state.llm_outputs


def test_out_of_order_sequence_rejected() -> None:
    run_id = "run-2"
    first = make_event(
        run_id, EventType.RUN_CREATED, 1, {"workflow_name": "report_workflow", "input": {}}
    )
    bad = make_event(run_id, EventType.RUN_STARTED, 3, {"worker_id": "w"})
    state = reduce_event(WorkflowState(run_id=run_id), first)
    with pytest.raises(ReducerError):
        reduce_event(state, bad)


def test_retry_from_failed() -> None:
    run_id = "run-3"
    events = [
        make_event(
            run_id, EventType.RUN_CREATED, 1, {"workflow_name": "report_workflow", "input": {}}
        ),
        make_event(run_id, EventType.RUN_STARTED, 2, {"worker_id": "w"}),
        make_event(
            run_id, EventType.RUN_FAILED, 3, {"error": "boom", "retryable": False, "step_id": None}
        ),
        make_event(run_id, EventType.RUN_RETRY_REQUESTED, 4, {"reason": "explicit_retry"}),
    ]
    state = reduce_events(run_id, events)
    assert state.status == RunStatus.PENDING.value
    assert state.error is None


def test_pending_tool_cleared_on_success() -> None:
    run_id = "run-4"
    events = [
        make_event(
            run_id, EventType.RUN_CREATED, 1, {"workflow_name": "report_workflow", "input": {}}
        ),
        make_event(run_id, EventType.RUN_STARTED, 2, {"worker_id": "w"}),
        make_event(
            run_id, EventType.STEP_STARTED, 3, {"step_id": "save_report", "step_name": "Save", "input": {}}
        ),
        make_event(
            run_id,
            EventType.TOOL_CALL_REQUESTED,
            4,
            {
                "step_id": "save_report",
                "tool_name": "save_report",
                "idempotency_key": "k1",
                "arguments": {"body": "x"},
            },
        ),
        make_event(
            run_id,
            EventType.TOOL_CALL_SUCCEEDED,
            5,
            {
                "step_id": "save_report",
                "tool_name": "save_report",
                "idempotency_key": "k1",
                "result": {"effect_id": "e1"},
            },
        ),
    ]
    state = reduce_events(run_id, events)
    assert state.pending_tool is None
    assert state.tools["k1"].status == "succeeded"
