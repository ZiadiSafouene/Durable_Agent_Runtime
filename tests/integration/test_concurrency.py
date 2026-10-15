from __future__ import annotations

import asyncio

import pytest

from app.storage.repositories import RunRepository, ToolRepository
from app.tools.executor import ToolExecutor
from tests.helpers import create_run

pytestmark = pytest.mark.mysql


async def test_concurrent_workers_skip_locked(container, settings) -> None:
    run_id = await create_run(container.session_factory, "Only one lease")

    async def claim(worker_id: str):
        async with container.session_factory() as session:
            repo = RunRepository(session)
            run = await repo.claim_next(worker_id, settings.lease_seconds)
            await session.commit()
            return run.id if run is not None else None

    first, second = await asyncio.gather(claim("w-a"), claim("w-b"))
    claimed = [item for item in (first, second) if item is not None]
    assert claimed == [run_id]


async def test_duplicate_tool_request_single_effect(container) -> None:
    run_id = await create_run(container.session_factory, "dup tool")
    executor: ToolExecutor = container.tool_executor
    key = f"{run_id}:save_report:save_report"

    async def once():
        return await executor.execute(
            run_id=run_id,
            step_id="save_report",
            tool_name="save_report",
            idempotency_key=key,
            arguments={"title": "t", "body": "hello world"},
        )

    results = await asyncio.gather(once(), once())
    assert results[0]["effect_id"] == results[1]["effect_id"]
    async with container.session_factory() as session:
        count = await ToolRepository(session).count_effects(run_id=run_id, tool_name="save_report")
    assert count == 1
