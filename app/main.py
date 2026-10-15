from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.runs import router as runs_router
from app.config import Settings, get_settings
from app.container import build_container
from app.observability import configure_logging
from app.workers.worker import Worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    configure_logging(settings.log_level)
    container = build_container(settings)
    app.state.container = container
    worker: Worker | None = None
    if settings.embedded_worker:
        worker = Worker(container.session_factory, container.engine, settings)
        await worker.start_background()
        app.state.worker = worker
    try:
        yield
    finally:
        if worker is not None:
            await worker.stop()
        await container.engine_db.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    app = FastAPI(
        title="Durable Agent Runtime",
        version="0.1.0",
        description=(
            "Crash-safe, replayable multi-step agent orchestration. "
            "Creates runs immediately; a worker executes them from the MySQL event log."
        ),
        lifespan=lifespan,
    )
    app.state.settings = resolved
    app.include_router(health_router)
    app.include_router(runs_router)
    return app


app = create_app()


def run() -> None:
    settings = get_settings()
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, reload=False)
