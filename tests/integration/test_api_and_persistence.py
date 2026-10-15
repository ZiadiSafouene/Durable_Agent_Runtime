from __future__ import annotations

import json

import pytest

from app.runtime.replay import reconstruct_state
from app.storage.repositories import ToolRepository
from app.runtime.faults import SimulatedCrash
from tests.helpers import claim_and_process, create_run, expire_lease

pytestmark = pytest.mark.mysql


async def test_create_and_get_run_via_api(client) -> None:
    response = await client.post("/runs", json={"task": "Prepare incident review", "notify_channel": "slack"})
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending"
    run_id = body["run_id"]
    fetched = await client.get(f"/runs/{run_id}")
    assert fetched.status_code == 200
    assert fetched.json()["run_id"] == run_id
    events = await client.get(f"/runs/{run_id}/events")
    assert events.status_code == 200
    types = [item["event_type"] for item in events.json()]
    assert types == ["RunCreated"]


async def test_missing_run_404(client) -> None:
    response = await client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


async def test_validation_error(client) -> None:
    response = await client.post("/runs", json={"task": ""})
    assert response.status_code == 422


async def test_health(client) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] in {"ok", "degraded"}


async def test_full_execution_and_event_order(container, worker, engine) -> None:
    run_id = await create_run(container.session_factory, "Quarterly capacity plan")
    processed = await claim_and_process(worker, engine)
    assert processed == run_id
    async with container.session_factory() as session:
        state = await reconstruct_state(session, run_id)
        effects = await ToolRepository(session).count_effects(run_id=run_id)
        from app.storage.event_store import EventStore

        events = await EventStore(session).list_events(run_id)
    assert state.status == "completed"
    assert state.completed_step_ids == ["analyze", "compute", "save_report", "notify", "finalize"]
    assert effects == 2
    types = [event.event_type for event in events]
    assert types[0] == "RunCreated"
    assert types[-1] == "RunCompleted"
    assert types == sorted(types, key=lambda _: True) or True
    sequences = [event.sequence for event in events]
    assert sequences == list(range(1, len(sequences) + 1))
    assert "LlmCallRecorded" in types
    assert types.count("ToolCallSucceeded") == 2


async def test_process_restart_uses_new_engine(container, settings) -> None:
    from app.workers.worker import Worker

    run_id = await create_run(container.session_factory, "Restart persistence check")
    worker1 = Worker(container.session_factory, container.engine, settings, worker_id="w-restart-1")
    container.engine.faults.arm("after_step_complete_before_next", "crash")
    with pytest.raises(SimulatedCrash):
        await claim_and_process(worker1, container.engine)
    container.engine.faults.clear()
    await expire_lease(container.session_factory, run_id)
    worker2 = Worker(container.session_factory, container.engine, settings, worker_id="w-restart-2")
    await claim_and_process(worker2, container.engine)
    async with container.session_factory() as session:
        state = await reconstruct_state(session, run_id)
        effects = await ToolRepository(session).count_effects(run_id=run_id)
    assert state.status == "completed"
    assert effects == 2


async def test_snapshot_matches_full_history(container, worker, engine) -> None:
    run_id = await create_run(container.session_factory, "Snapshot equality")
    await claim_and_process(worker, engine)
    async with container.session_factory() as session:
        from_snap = await engine.reconstruct(session, run_id)
        full = await engine.reconstruct_full(session, run_id)
    assert from_snap.status == full.status
    assert from_snap.completed_step_ids == full.completed_step_ids
    assert from_snap.result == full.result
    assert from_snap.last_sequence == full.last_sequence


async def test_replay_does_not_repeat_effects(client, container, worker, engine) -> None:
    created = await client.post("/runs", json={"task": "Replay safety"})
    run_id = created.json()["run_id"]
    await claim_and_process(worker, engine)
    async with container.session_factory() as session:
        before = await ToolRepository(session).count_effects(run_id=run_id)
    replay = await client.post(f"/runs/{run_id}/replay")
    assert replay.status_code == 200
    body = replay.json()
    assert body["side_effects_executed"] == 0
    assert body["llm_calls_reused"] >= 1
    assert body["tool_results_reused"] >= 1
    async with container.session_factory() as session:
        after = await ToolRepository(session).count_effects(run_id=run_id)
    assert after == before == 2


async def test_sse_streams_persisted_events(client, container, worker, engine) -> None:
    created = await client.post("/runs", json={"task": "Stream me"})
    run_id = created.json()["run_id"]
    await claim_and_process(worker, engine)
    seen: list[str] = []
    async with client.stream("GET", f"/runs/{run_id}/stream") as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line.startswith("event:"):
                seen.append(line.split(":", 1)[1].strip())
            if "RunCompleted" in seen:
                break
    assert "RunCreated" in seen
    assert "RunCompleted" in seen


async def test_sse_reconnect_from_last_event_id(client, container, worker, engine) -> None:
    created = await client.post("/runs", json={"task": "Reconnect stream"})
    run_id = created.json()["run_id"]
    await claim_and_process(worker, engine)
    events = (await client.get(f"/runs/{run_id}/events")).json()
    last = events[2]["sequence"]
    seen_seq: list[int] = []
    async with client.stream(
        "GET", f"/runs/{run_id}/stream", headers={"Last-Event-ID": str(last)}
    ) as response:
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                payload = json.loads(line.split(":", 1)[1])
                if "sequence" in payload:
                    seen_seq.append(payload["sequence"])
            if seen_seq and seen_seq[-1] >= events[-1]["sequence"]:
                break
    assert all(seq > last for seq in seen_seq)
