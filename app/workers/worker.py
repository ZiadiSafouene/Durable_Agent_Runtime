from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.observability import log_extra
from app.runtime.engine import Engine, LostLeaseError
from app.runtime.faults import SimulatedCrash
from app.storage.repositories import RunRepository

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        engine: Engine,
        settings: Settings,
        worker_id: str | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.engine = engine
        self.settings = settings
        self.worker_id = worker_id or f"worker-{uuid4().hex[:8]}"
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def request_stop(self) -> None:
        self._stop.set()

    async def start_background(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run_forever(), name=f"worker-{self.worker_id}")

    async def stop(self) -> None:
        self.request_stop()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None

    async def run_forever(self) -> None:
        logger.info("worker_started", **log_extra(worker_id=self.worker_id))
        while not self._stop.is_set():
            try:
                claimed = await self.claim_and_run()
            except SimulatedCrash:
                logger.warning("worker_simulated_crash", **log_extra(worker_id=self.worker_id))
                return
            except LostLeaseError:
                logger.info("worker_lost_lease", **log_extra(worker_id=self.worker_id))
                claimed = True
            except Exception:
                logger.exception("worker_loop_error", **log_extra(worker_id=self.worker_id))
                claimed = False
            if not claimed:
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.settings.worker_poll_interval_seconds
                    )
                except TimeoutError:
                    pass
        logger.info("worker_stopped", **log_extra(worker_id=self.worker_id))

    async def claim_and_run(self) -> bool:
        async with self.session_factory() as session:
            repo = RunRepository(session)
            run = await repo.claim_next(self.worker_id, self.settings.lease_seconds)
            if run is None:
                await session.commit()
                return False
            run_id = run.id
            await session.commit()
        logger.info("run_claimed", **log_extra(run_id=run_id, worker_id=self.worker_id))
        await self.engine.process_run(run_id, self.worker_id)
        return True
