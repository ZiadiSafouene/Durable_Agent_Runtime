from __future__ import annotations

from typing import Any
from uuid import uuid4

from app.models.tables import ToolEffect
from app.tools.base import ToolContext


class SaveReportTool:
    name = "save_report"

    async def execute(self, context: ToolContext, arguments: dict[str, Any]) -> dict[str, Any]:
        body = str(arguments["body"])
        title = str(arguments.get("title") or "report")
        effect_id = str(uuid4())
        context.session.add(
            ToolEffect(
                id=effect_id,
                idempotency_key=context.idempotency_key,
                run_id=context.run_id,
                tool_name=self.name,
                payload={"title": title, "body": body, "bytes": len(body.encode("utf-8"))},
            )
        )
        await context.session.flush()
        return {"effect_id": effect_id, "title": title, "bytes": len(body.encode("utf-8"))}
