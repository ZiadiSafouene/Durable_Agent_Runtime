from __future__ import annotations

import asyncio
import logging

from app.config import get_settings
from app.container import build_container
from app.observability import configure_logging
from app.workers.worker import Worker


async def _amain() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    container = build_container(settings)
    worker = Worker(container.session_factory, container.engine, settings)
    try:
        await worker.run_forever()
    finally:
        await container.engine_db.dispose()


def main() -> None:
    logging.getLogger(__name__).info("starting durable worker")
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
