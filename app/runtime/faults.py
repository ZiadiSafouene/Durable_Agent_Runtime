from __future__ import annotations

import asyncio
from typing import Literal

Action = Literal["crash", "pause", "skip"]


class SimulatedCrash(Exception):
    def __init__(self, point: str) -> None:
        super().__init__(f"simulated crash at {point}")
        self.point = point


class FaultController:
    """Deterministic fault injection for reliability tests. Not used in production."""

    def __init__(self) -> None:
        self._actions: dict[str, Action] = {}
        self._barriers: dict[str, asyncio.Event] = {}
        self.hits: list[str] = []

    def arm(self, point: str, action: Action) -> None:
        self._actions[point] = action
        if action == "pause":
            self._barriers[point] = asyncio.Event()

    def release(self, point: str) -> None:
        barrier = self._barriers.get(point)
        if barrier is not None:
            barrier.set()

    def clear(self) -> None:
        self._actions.clear()
        self._barriers.clear()
        self.hits.clear()

    async def hit(self, point: str) -> None:
        self.hits.append(point)
        action = self._actions.get(point)
        if action == "crash":
            raise SimulatedCrash(point)
        if action == "pause":
            await self._barriers[point].wait()
