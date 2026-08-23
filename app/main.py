from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings, get_settings
from app.config.scheduler import ScheduledJob, build_scheduler
from app.handlers.coursera_handler import run_coursera_ingestion
from app.handlers.datacamp_handler import run_datacamp_ingestion
from app.handlers.harvard_hmm_handler import run_harvard_hmm_ingestion
from app.handlers.harvard_spark_handler import run_harvard_spark_ingestion
from app.handlers.levelup_handler import run_levelup_ingestion
from app.handlers.linkedin_handler import run_linkedin_ingestion
from app.handlers.skillup_handler import run_skillup_ingestion
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
            jobs: dict[str, ScheduledJob] = {}

            if config.levelup_configured:
                async def scheduled_levelup_ingestion() -> object:
                    return await run_levelup_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["levelup"] = scheduled_levelup_ingestion

            if config.skillup_configured:
                async def scheduled_skillup_ingestion() -> object:
                    return await run_skillup_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["skillup"] = scheduled_skillup_ingestion

            if config.datacamp_configured:
                async def scheduled_datacamp_ingestion() -> object:
                    return await run_datacamp_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["datacamp"] = scheduled_datacamp_ingestion

            if config.coursera_configured:
                async def scheduled_coursera_ingestion() -> object:
                    return await run_coursera_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["coursera"] = scheduled_coursera_ingestion

            if config.linkedin_configured:
                async def scheduled_linkedin_ingestion() -> object:
                    return await run_linkedin_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["linkedin"] = scheduled_linkedin_ingestion

            if config.harvard_hmm_configured:
                async def scheduled_harvard_hmm_ingestion() -> object:
                    return await run_harvard_hmm_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["harvard_hmm"] = scheduled_harvard_hmm_ingestion

            if config.harvard_spark_configured:
                async def scheduled_harvard_spark_ingestion() -> object:
                    return await run_harvard_spark_ingestion(
                        config,
                        checkpoint_store=store,
                        bronze_writer=writer,
                    )

                jobs["harvard_spark"] = scheduled_harvard_spark_ingestion

            if not jobs:
                raise ValueError("Scheduler enabled but no vendor is fully configured")
            scheduler = build_scheduler(config, jobs)
            scheduler.start()
            logger.info(
                "Vendor scheduler started vendors=%s schedule=%s timezone=%s",
                ",".join(jobs),
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
