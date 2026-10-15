from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "mysql+asyncmy://durable:durable@127.0.0.1:3306/durable_agent"
    database_sync_url: str | None = None
    test_database_url: str = "mysql+asyncmy://durable:durable@127.0.0.1:3306/durable_agent_test"
    test_database_sync_url: str | None = None
    lease_seconds: int = 30
    worker_poll_interval_seconds: float = 0.4
    worker_heartbeat_seconds: float = 10.0
    max_tool_attempts: int = 3
    snapshot_every_events: int = 50
    embedded_worker: bool = True
    log_level: str = "INFO"
    host: str = "127.0.0.1"
    port: int = 8000
    retry_base_delay_seconds: float = 0.05
    retry_max_delay_seconds: float = 2.0
    sse_poll_interval_seconds: float = 0.25
    sse_heartbeat_seconds: float = 15.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
