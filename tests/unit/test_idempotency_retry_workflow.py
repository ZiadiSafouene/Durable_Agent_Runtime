from __future__ import annotations

from app.runtime.idempotency import tool_idempotency_key
from app.runtime.retry import RetryPolicy
from app.runtime.workflow import STEPS, next_step


def test_idempotency_key_stable_across_attempts() -> None:
    first = tool_idempotency_key("run", "save_report", "save_report")
    second = tool_idempotency_key("run", "save_report", "save_report")
    assert first == second
    assert first == "run:save_report:save_report"


def test_retry_policy_exponential_and_cap() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay_seconds=0.05, max_delay_seconds=0.2)
    assert policy.delay_for_attempt(1) == 0.05
    assert policy.delay_for_attempt(2) == 0.10
    assert policy.delay_for_attempt(3) == 0.20
    assert policy.delay_for_attempt(4) == 0.20
    assert policy.should_retry(2, True) is True
    assert policy.should_retry(5, True) is False
    assert policy.should_retry(1, False) is False


def test_workflow_step_order() -> None:
    assert [s.id for s in STEPS] == ["analyze", "compute", "save_report", "notify", "finalize"]
    assert next_step([]).id == "analyze"
    assert next_step(["analyze", "compute"]).id == "save_report"
    assert next_step([s.id for s in STEPS]) is None
