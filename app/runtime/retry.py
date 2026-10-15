from __future__ import annotations

from dataclasses import dataclass


class RetryableError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class PermanentError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.05
    max_delay_seconds: float = 2.0

    def delay_for_attempt(self, attempt: int) -> float:
        if attempt < 1:
            return 0.0
        delay = self.base_delay_seconds * (2 ** (attempt - 1))
        return min(delay, self.max_delay_seconds)

    def should_retry(self, attempt: int, retryable: bool) -> bool:
        return retryable and attempt < self.max_attempts
