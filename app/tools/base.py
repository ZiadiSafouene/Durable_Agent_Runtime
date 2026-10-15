from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class ToolContext:
    session: AsyncSession
    run_id: str
    step_id: str
    idempotency_key: str


class Tool(Protocol):
    name: str

    async def execute(self, context: ToolContext, arguments: dict[str, Any]) -> dict[str, Any]:
        ...


class ToolRegistry:
    def __init__(self, tools: list[Tool] | None = None) -> None:
        self._tools: dict[str, Tool] = {}
        for tool in tools or []:
            self.register(tool)

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"unknown tool {name}") from exc

    def names(self) -> list[str]:
        return list(self._tools)
