from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import OperationStatus, RunStatus
from app.models.tables import ToolEffect, ToolOperation, WorkflowRun, WorkflowSnapshot


class RunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, workflow_name: str, input_payload: dict[str, Any]) -> WorkflowRun:
        run = WorkflowRun(
            id=str(uuid4()),
            workflow_name=workflow_name,
            status=RunStatus.PENDING.value,
            input_payload=input_payload,
            attempt_count=0,
        )
        self.session.add(run)
        await self.session.flush()
        return run

    async def get(self, run_id: str) -> WorkflowRun | None:
        return await self.session.get(WorkflowRun, run_id)

    async def get_for_update(self, run_id: str) -> WorkflowRun | None:
        result = await self.session.scalars(
            select(WorkflowRun).where(WorkflowRun.id == run_id).with_for_update()
        )
        return result.first()

    async def claim_next(self, worker_id: str, lease_seconds: int) -> WorkflowRun | None:
        now = datetime.now(UTC).replace(tzinfo=None)
        stmt = (
            select(WorkflowRun)
            .where(
                WorkflowRun.status.in_([RunStatus.PENDING.value, RunStatus.RUNNING.value]),
                or_(WorkflowRun.lease_expires_at.is_(None), WorkflowRun.lease_expires_at < now),
            )
            .order_by(WorkflowRun.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        run = (await self.session.scalars(stmt)).first()
        if run is None:
            return None
        run.lease_owner = worker_id
        run.lease_expires_at = now + timedelta(seconds=lease_seconds)
        run.status = RunStatus.RUNNING.value
        run.attempt_count = int(run.attempt_count) + 1
        await self.session.flush()
        return run

    async def heartbeat(self, run_id: str, worker_id: str, lease_seconds: int) -> bool:
        run = await self.get_for_update(run_id)
        if run is None or run.lease_owner != worker_id:
            return False
        now = datetime.now(UTC).replace(tzinfo=None)
        if run.lease_expires_at is not None and run.lease_expires_at < now:
            return False
        run.lease_expires_at = now + timedelta(seconds=lease_seconds)
        await self.session.flush()
        return True

    async def owns_lease(self, run_id: str, worker_id: str) -> bool:
        run = await self.get(run_id)
        if run is None or run.lease_owner != worker_id:
            return False
        now = datetime.now(UTC).replace(tzinfo=None)
        if run.lease_expires_at is not None and run.lease_expires_at < now:
            return False
        return True

    async def release(self, run: WorkflowRun) -> None:
        run.lease_owner = None
        run.lease_expires_at = None
        await self.session.flush()


class ToolRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_key(self, idempotency_key: str) -> ToolOperation | None:
        result = await self.session.scalars(
            select(ToolOperation).where(ToolOperation.idempotency_key == idempotency_key)
        )
        return result.first()

    async def get_by_key_for_update(self, idempotency_key: str) -> ToolOperation | None:
        result = await self.session.scalars(
            select(ToolOperation)
            .where(ToolOperation.idempotency_key == idempotency_key)
            .with_for_update()
        )
        return result.first()

    async def ensure_requested(
        self,
        *,
        idempotency_key: str,
        run_id: str,
        step_id: str,
        tool_name: str,
        request_payload: dict[str, Any],
    ) -> ToolOperation:
        existing = await self.get_by_key(idempotency_key)
        if existing is not None:
            return existing
        nested = await self.session.begin_nested()
        try:
            op = ToolOperation(
                idempotency_key=idempotency_key,
                run_id=run_id,
                step_id=step_id,
                tool_name=tool_name,
                status=OperationStatus.REQUESTED.value,
                request_payload=request_payload,
                attempt_count=0,
            )
            self.session.add(op)
            await self.session.flush()
            await nested.commit()
            return op
        except IntegrityError:
            await nested.rollback()
            recovered = await self.get_by_key(idempotency_key)
            if recovered is None:
                raise
            return recovered

    async def count_effects(self, run_id: str | None = None, tool_name: str | None = None) -> int:
        stmt = select(ToolEffect)
        if run_id is not None:
            stmt = stmt.where(ToolEffect.run_id == run_id)
        if tool_name is not None:
            stmt = stmt.where(ToolEffect.tool_name == tool_name)
        rows = (await self.session.scalars(stmt)).all()
        return len(rows)

    async def list_effects(self, run_id: str) -> list[ToolEffect]:
        result = await self.session.scalars(select(ToolEffect).where(ToolEffect.run_id == run_id))
        return list(result.all())


class SnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def latest(self, run_id: str) -> WorkflowSnapshot | None:
        result = await self.session.scalars(
            select(WorkflowSnapshot)
            .where(WorkflowSnapshot.run_id == run_id)
            .order_by(WorkflowSnapshot.sequence.desc())
            .limit(1)
        )
        return result.first()

    async def save(self, run_id: str, sequence: int, state_payload: dict[str, Any]) -> WorkflowSnapshot:
        existing = await self.session.scalar(
            select(WorkflowSnapshot).where(
                WorkflowSnapshot.run_id == run_id, WorkflowSnapshot.sequence == sequence
            )
        )
        if existing is not None:
            return existing
        snap = WorkflowSnapshot(run_id=run_id, sequence=sequence, state_payload=state_payload)
        self.session.add(snap)
        await self.session.flush()
        return snap
