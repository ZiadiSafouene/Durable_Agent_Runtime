from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.models.enums import EventType
from app.models.tables import WorkflowEvent
from app.runtime.engine import Engine
from app.schemas.events import EVENT_PAYLOAD_TYPES, NewEvent
from app.storage.event_store import EventStore
from app.storage.repositories import RunRepository
from app.workers.worker import Worker


def make_event(run_id: str, event_type: EventType, sequence: int, payload: dict) -> WorkflowEvent:
    EVENT_PAYLOAD_TYPES[event_type].model_validate(payload)
    return WorkflowEvent(
        id=str(uuid4()),
        run_id=run_id,
        event_type=event_type.value,
        sequence=sequence,
        payload=payload,
        schema_version=1,
        created_at=datetime.now(UTC).replace(tzinfo=None),
    )


async def create_run(session_factory, task: str = "Write a weekly ops report", channel: str = "email"):
    async with session_factory() as session:
        run = await RunRepository(session).create(
            "report_workflow", {"task": task, "notify_channel": channel}
        )
        await EventStore(session).append(
            run.id,
            [
                NewEvent(
                    event_type=EventType.RUN_CREATED,
                    payload={"workflow_name": "report_workflow", "input": run.input_payload},
                )
            ],
        )
        await session.commit()
        return run.id


async def expire_lease(session_factory, run_id: str) -> None:
    async with session_factory() as session:
        run = await RunRepository(session).get_for_update(run_id)
        if run is not None:
            run.lease_expires_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=5)
            run.lease_owner = None
            await session.commit()


async def claim_and_process(worker: Worker, engine: Engine) -> str | None:
    async with worker.session_factory() as session:
        run = await RunRepository(session).claim_next(worker.worker_id, worker.settings.lease_seconds)
        if run is None:
            await session.commit()
            return None
        run_id = run.id
        await session.commit()
    await engine.process_run(run_id, worker.worker_id)
    return run_id
