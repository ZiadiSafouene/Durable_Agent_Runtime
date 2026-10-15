from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.runtime.llm import FakeLlmProvider
from app.runtime.workflow import analysis_from_llm, build_report_body, compute_report


@dataclass
class InMemoryEffectLog:
    effects: list[dict[str, Any]] = field(default_factory=list)

    def apply(self, tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        effect_id = f"mem-{len(self.effects) + 1}"
        self.effects.append({"id": effect_id, "tool_name": tool_name, "payload": payload})
        return {"effect_id": effect_id, **payload}


async def run_in_memory_workflow(task: str, notify_channel: str = "email") -> dict[str, Any]:
    llm = FakeLlmProvider()
    effects = InMemoryEffectLog()
    prompt = f"Analyze the following task for a short report plan.\nTASK:\n{task}"
    response = await llm.complete(prompt, purpose="analyze", params={"temperature": 0})
    analysis = analysis_from_llm({"response": {"text": response.text}})
    computed = compute_report(task, analysis)
    body = build_report_body(task, computed)
    saved = effects.apply("save_report", {"title": computed["report_id"], "bytes": len(body.encode())})
    notified = effects.apply(
        "notify",
        {"channel": notify_channel, "message": f"Report ready for '{task[:80]}'"},
    )
    return {
        "report_id": computed["report_id"],
        "score": computed["score"],
        "save": saved,
        "notify": notified,
        "summary": analysis.get("summary"),
        "effect_count": len(effects.effects),
    }
