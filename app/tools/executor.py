from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.enums import OperationStatus
from app.observability import log_extra
from app.runtime.retry import PermanentError, RetryableError
from app.storage.repositories import ToolRepository
from app.tools.base import Tool, ToolContext, ToolRegistry

logger = logging.getLogger(__name__)


class ToolExecutor:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], registry: ToolRegistry) -> None:
        self.session_factory = session_factory
        self.registry = registry

    async def execute(
        self,
        *,
        run_id: str,
        step_id: str,
        tool_name: str,
        idempotency_key: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Commit the mock effect and the succeeded operation row in one transaction."""
        async with self.session_factory() as session:
            repo = ToolRepository(session)
            op = await repo.get_by_key_for_update(idempotency_key)
            if op is None:
                try:
                    op = await repo.ensure_requested(
                        idempotency_key=idempotency_key,
                        run_id=run_id,
                        step_id=step_id,
                        tool_name=tool_name,
                        request_payload=arguments,
                    )
                except IntegrityError:
                    await session.rollback()
                    async with self.session_factory() as other:
                        recovered = await ToolRepository(other).get_by_key(idempotency_key)
                        if recovered is not None and recovered.status == OperationStatus.SUCCEEDED.value:
                            return recovered.result_payload or {}
                    raise
                op = await repo.get_by_key_for_update(idempotency_key)
            if op is None:
                raise RuntimeError("tool operation disappeared after insert")
            if op.status == OperationStatus.SUCCEEDED.value:
                await session.commit()
                return op.result_payload or {}

            op.status = OperationStatus.EXECUTING.value
            op.attempt_count = int(op.attempt_count) + 1
            await session.flush()

            tool: Tool = self.registry.get(tool_name)
            context = ToolContext(
                session=session, run_id=run_id, step_id=step_id, idempotency_key=idempotency_key
            )
            try:
                result = await tool.execute(context, arguments)
            except RetryableError as exc:
                op.status = OperationStatus.FAILED.value
                op.error = str(exc)
                await session.commit()
                raise
            except Exception as exc:
                op.status = OperationStatus.FAILED.value
                op.error = str(exc)
                await session.commit()
                raise PermanentError(str(exc)) from exc

            op.status = OperationStatus.SUCCEEDED.value
            op.result_payload = result
            op.error = None
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                async with self.session_factory() as retry_session:
                    recovered = await ToolRepository(retry_session).get_by_key(idempotency_key)
                    if recovered is not None and recovered.status == OperationStatus.SUCCEEDED.value:
                        return recovered.result_payload or {}
                raise
            logger.info(
                "tool_succeeded",
                **log_extra(run_id=run_id, operation_id=idempotency_key, step_id=step_id),
            )
            return result
