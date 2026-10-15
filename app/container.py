from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.runtime.engine import Engine
from app.runtime.faults import FaultController
from app.runtime.llm import FakeLlmProvider, LlmProvider
from app.runtime.retry import RetryPolicy
from app.storage.database import create_engine_from_url, create_session_factory
from app.tools.executor import ToolExecutor
from app.tools.mock_notification import NotifyTool
from app.tools.save_report import SaveReportTool
from app.tools.base import ToolRegistry


@dataclass
class AppContainer:
    settings: Settings
    engine_db: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    llm: LlmProvider
    tools: ToolRegistry
    notify_tool: NotifyTool
    tool_executor: ToolExecutor
    faults: FaultController
    engine: Engine


def build_container(
    settings: Settings,
    *,
    database_url: str | None = None,
    llm: LlmProvider | None = None,
    faults: FaultController | None = None,
) -> AppContainer:
    engine_db = create_engine_from_url(database_url or settings.database_url)
    session_factory = create_session_factory(engine_db)
    notify = NotifyTool()
    tools = ToolRegistry([SaveReportTool(), notify])
    executor = ToolExecutor(session_factory, tools)
    controller = faults or FaultController()
    provider = llm or FakeLlmProvider()
    engine = Engine(
        session_factory=session_factory,
        tool_executor=executor,
        llm=provider,
        policy=RetryPolicy(
            max_attempts=settings.max_tool_attempts,
            base_delay_seconds=settings.retry_base_delay_seconds,
            max_delay_seconds=settings.retry_max_delay_seconds,
        ),
        snapshot_every=settings.snapshot_every_events,
        faults=controller,
    )
    return AppContainer(
        settings=settings,
        engine_db=engine_db,
        session_factory=session_factory,
        llm=provider,
        tools=tools,
        notify_tool=notify,
        tool_executor=executor,
        faults=controller,
        engine=engine,
    )
