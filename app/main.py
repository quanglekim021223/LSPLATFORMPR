from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings, get_settings
from app.config.scheduler import build_scheduler
from app.handlers.levelup_handler import run_levelup_ingestion
from app.repositories.checkpoint_repository import CheckpointStore
from app.routers.health_router import build_health_router
from app.routers.job_router import build_job_router
from app.storage import BronzeWriter, LocalBronzeWriter

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
) -> FastAPI:
    config = settings or get_settings()
    store = checkpoint_store or CheckpointStore(config.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(config.bronze_local_path)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
        await store.initialize()
        scheduler = None
        if config.scheduler_may_run:
            config.validate_levelup_runtime()

            async def scheduled_ingestion() -> object:
                return await run_levelup_ingestion(
                    config,
                    checkpoint_store=store,
                    bronze_writer=writer,
                )

            scheduler = build_scheduler(config, scheduled_ingestion)
            scheduler.start()
            logger.info(
                "LevelUP scheduler started schedule=%s timezone=%s",
                config.ingestion_time,
                config.ingestion_timezone,
            )
        application.state.settings = config
        application.state.checkpoint_store = store
        application.state.bronze_writer = writer
        application.state.scheduler = scheduler
        try:
            yield
        finally:
            if scheduler is not None:
                scheduler.shutdown(wait=False)

    application = FastAPI(
        title="FSA Learning Vendor Ingestion",
        version="0.1.0",
        lifespan=lifespan,
    )

    application.include_router(build_health_router(store))
    application.include_router(build_job_router(store))

    return application


app = create_app()
