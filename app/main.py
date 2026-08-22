from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from app.checkpoint import CheckpointStore
from app.config import Settings, get_settings
from app.levelup import run_levelup_ingestion
from app.models import RunSummary
from app.scheduler import build_scheduler
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

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/ready")
    async def ready() -> dict[str, str]:
        if not await store.is_ready():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="checkpoint store is not ready",
            )
        return {"status": "ready"}

    @application.get("/jobs/levelup/latest", response_model=RunSummary)
    async def latest_levelup_job() -> RunSummary:
        summary = await store.latest_run("levelup")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No LevelUP run found",
            )
        return summary

    return application


app = create_app()
