from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import EventType
from app.models.tables import WorkflowEvent, WorkflowRun
from app.schemas.events import NewEvent, parse_payload


class EventStoreError(Exception):
    pass


class EventStore:
    """Append-only event log. Existing rows are never updated or deleted by this class."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def append(self, run_id: str, events: list[NewEvent]) -> list[WorkflowEvent]:
        if not events:
            return []
        await self.session.execute(select(WorkflowRun).where(WorkflowRun.id == run_id).with_for_update())
        current = await self.session.scalar(
            select(func.max(WorkflowEvent.sequence)).where(WorkflowEvent.run_id == run_id)
        )
        sequence = int(current or 0)
        last_id: str | None = None
        stored: list[WorkflowEvent] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        for item in events:
            parse_payload(item.event_type, item.payload)
            sequence += 1
            row = WorkflowEvent(
                id=str(uuid4()),
                run_id=run_id,
                event_type=item.event_type.value,
                sequence=sequence,
                payload=item.payload,
                schema_version=item.schema_version,
                correlation_id=item.correlation_id or run_id,
                causation_id=item.causation_id or last_id,
                created_at=now,
            )
            self.session.add(row)
            stored.append(row)
            last_id = row.id
        await self.session.flush()
        return stored

    async def list_events(self, run_id: str, after_sequence: int = 0) -> list[WorkflowEvent]:
        stmt: Select[tuple[WorkflowEvent]] = (
            select(WorkflowEvent)
            .where(WorkflowEvent.run_id == run_id, WorkflowEvent.sequence > after_sequence)
            .order_by(WorkflowEvent.sequence.asc())
        )
        result = await self.session.scalars(stmt)
        return list(result.all())

    async def max_sequence(self, run_id: str) -> int:
        current = await self.session.scalar(
            select(func.max(WorkflowEvent.sequence)).where(WorkflowEvent.run_id == run_id)
        )
        return int(current or 0)

    @staticmethod
    def typed(event: WorkflowEvent) -> EventType:
        return EventType(event.event_type)
