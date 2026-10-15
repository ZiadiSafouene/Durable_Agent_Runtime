from __future__ import annotations

from fastapi import APIRouter, Request
from sqlalchemy import text

from app.schemas.api import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    container = request.app.state.container
    try:
        async with container.session_factory() as session:
            await session.execute(text("SELECT 1"))
        return HealthResponse(status="ok", database="up")
    except Exception:
        return HealthResponse(status="degraded", database="down")
