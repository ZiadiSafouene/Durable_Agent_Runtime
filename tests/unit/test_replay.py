from __future__ import annotations

from app.models.enums import EventType
from app.runtime.state import reduce_events
from app.runtime.workflow import compute_report
from tests.helpers import make_event


def test_replay_state_does_not_need_tools() -> None:
    run_id = "run-replay"
    events = [
        make_event(
            run_id,
            EventType.RUN_CREATED,
            1,
            {"workflow_name": "report_workflow", "input": {"task": "alpha beta"}},
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
                "prompt_ref": "p",
                "request_params": {"temperature": 0},
                "response": {"text": "summary=processed:abc; keywords=alpha,beta; purpose=analyze; length=10"},
            },
        ),
        make_event(
            run_id,
            EventType.STEP_COMPLETED,
            5,
            {"step_id": "analyze", "output": {"summary": "processed:abc", "keywords": ["alpha", "beta"]}},
        ),
        make_event(
            run_id, EventType.STEP_STARTED, 6, {"step_id": "compute", "step_name": "Compute", "input": {}}
        ),
        make_event(
            run_id,
            EventType.STEP_COMPLETED,
            7,
            {"step_id": "compute", "output": compute_report("alpha beta", {"keywords": ["alpha", "beta"]})},
        ),
        make_event(run_id, EventType.RUN_COMPLETED, 8, {"result": {"ok": True}}),
    ]
    state = reduce_events(run_id, events)
    assert state.llm_outputs["analyze"]["provider"] == "fake"
    assert state.step_outputs["compute"]["word_count"] == 2
    assert state.status == "completed"
