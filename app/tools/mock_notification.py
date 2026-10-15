from __future__ import annotations

from typing import Any
from uuid import uuid4

from app.models.tables import ToolEffect
from app.tools.base import ToolContext
from app.runtime.retry import RetryableError


class NotifyTool:
    name = "notify"

    def __init__(self) -> None:
        self.fail_next_retryable = 0
        self.fail_next_permanent = 0

    async def execute(self, context: ToolContext, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.fail_next_permanent > 0:
            self.fail_next_permanent -= 1
            raise RuntimeError("permanent notification failure")
        if self.fail_next_retryable > 0:
            self.fail_next_retryable -= 1
            raise RetryableError("retryable notification failure")
        channel = str(arguments.get("channel") or "email")
        message = str(arguments["message"])
        effect_id = str(uuid4())
        context.session.add(
            ToolEffect(
                id=effect_id,
                idempotency_key=context.idempotency_key,
                run_id=context.run_id,
                tool_name=self.name,
                payload={"channel": channel, "message": message},
            )
        )
        await context.session.flush()
        return {"effect_id": effect_id, "channel": channel, "delivered": True}
