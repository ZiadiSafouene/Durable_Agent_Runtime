from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class LlmResponse:
    provider: str
    model: str
    text: str
    usage: dict[str, Any]


class LlmProvider(Protocol):
    provider_id: str
    model: str

    async def complete(self, prompt: str, *, purpose: str, params: dict[str, Any] | None = None) -> LlmResponse:
        ...


class FakeLlmProvider:
    """Deterministic provider for tests and demos. No network, no API key."""

    provider_id = "fake"
    model = "fake-1"

    def __init__(self, *, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def complete(self, prompt: str, *, purpose: str, params: dict[str, Any] | None = None) -> LlmResponse:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("injected llm failure")
        digest = hashlib.sha256(f"{purpose}\n{prompt}".encode()).hexdigest()[:16]
        words = [w for w in prompt.replace(",", " ").split() if w]
        keywords = words[:5]
        text = (
            f"summary=processed:{digest}; "
            f"keywords={','.join(keywords)}; "
            f"purpose={purpose}; length={len(prompt)}"
        )
        return LlmResponse(
            provider=self.provider_id,
            model=self.model,
            text=text,
            usage={"prompt_chars": len(prompt), "completion_chars": len(text)},
        )


def parse_fake_plan(text: str) -> dict[str, Any]:
    parts: dict[str, str] = {}
    for chunk in text.split(";"):
        chunk = chunk.strip()
        if "=" in chunk:
            key, value = chunk.split("=", 1)
            parts[key.strip()] = value.strip()
    return {
        "summary": parts.get("summary", text),
        "keywords": [k for k in parts.get("keywords", "").split(",") if k],
        "purpose": parts.get("purpose"),
        "length": parts.get("length"),
        "raw": text,
    }
