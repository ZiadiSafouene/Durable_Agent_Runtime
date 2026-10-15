from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Literal

from app.runtime.llm import parse_fake_plan

StepKind = Literal["llm", "compute", "tool", "finalize"]


@dataclass(frozen=True)
class StepDef:
    id: str
    name: str
    kind: StepKind
    tool_name: str | None = None


REPORT_WORKFLOW = "report_workflow"

STEPS: list[StepDef] = [
    StepDef("analyze", "Analyze task", "llm"),
    StepDef("compute", "Compute report metrics", "compute"),
    StepDef("save_report", "Save report", "tool", tool_name="save_report"),
    StepDef("notify", "Send notification", "tool", tool_name="notify"),
    StepDef("finalize", "Finalize result", "finalize"),
]


def get_workflow(name: str) -> list[StepDef]:
    if name != REPORT_WORKFLOW:
        raise ValueError(f"unknown workflow {name}")
    return STEPS


def next_step(completed: list[str], workflow: list[StepDef] | None = None) -> StepDef | None:
    steps = workflow or STEPS
    done = set(completed)
    for step in steps:
        if step.id not in done:
            return step
    return None


def compute_report(task: str, analysis: dict[str, Any]) -> dict[str, Any]:
    words = [w for w in task.split() if w]
    digest = hashlib.sha256(task.encode("utf-8")).hexdigest()[:12]
    score = min(100, 10 + len(words) * 3 + len(analysis.get("keywords", [])) * 5)
    return {
        "report_id": f"rpt_{digest}",
        "word_count": len(words),
        "score": score,
        "summary": analysis.get("summary"),
        "keywords": analysis.get("keywords", []),
    }


def build_report_body(task: str, computed: dict[str, Any]) -> str:
    keywords = ", ".join(computed.get("keywords") or [])
    return (
        f"# Report {computed['report_id']}\n"
        f"Task: {task}\n"
        f"Score: {computed['score']}\n"
        f"Words: {computed['word_count']}\n"
        f"Keywords: {keywords}\n"
        f"Summary: {computed.get('summary')}\n"
    )


def analysis_from_llm(response: dict[str, Any]) -> dict[str, Any]:
    text = response.get("response", {}).get("text") or response.get("text") or ""
    parsed = parse_fake_plan(text)
    return parsed
