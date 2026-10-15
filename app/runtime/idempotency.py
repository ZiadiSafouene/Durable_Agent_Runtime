from __future__ import annotations


def tool_idempotency_key(run_id: str, step_id: str, tool_name: str) -> str:
    """Stable key for a logical tool operation. Retries must reuse this value."""
    return f"{run_id}:{step_id}:{tool_name}"


def llm_prompt_ref(run_id: str, step_id: str, prompt: str) -> str:
    digest = _fnv1a(prompt)
    return f"{run_id}:{step_id}:{digest}"


def _fnv1a(value: str) -> str:
    h = 2166136261
    for ch in value.encode("utf-8"):
        h ^= ch
        h = (h * 16777619) % 2**32
    return f"{h:08x}"
