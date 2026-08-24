from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings, get_settings
from app.config.scheduler import ScheduledJob, build_scheduler
from app.handlers.coursera_handler import run_coursera_ingestion
from app.handlers.datacamp_handler import run_datacamp_ingestion
from app.handlers.fams_handler import run_fams_ingestion
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
IngestionRunner = Callable[..., Awaitable[object]]


def _configure_application_logging(level: str) -> None:
    app_logger = logging.getLogger("app")
    app_logger.setLevel(level)
    if app_logger.handlers or logging.getLogger().handlers:
        return

    uvicorn_handlers = (
        logging.getLogger("uvicorn.error").handlers
        or logging.getLogger("uvicorn").handlers
    )
    if uvicorn_handlers:
        app_logger.handlers = list(uvicorn_handlers)
        app_logger.propagate = False


def _scheduled_jobs(
    config: Settings,
    store: CheckpointStore,
    writer: BronzeWriter,
) -> dict[str, ScheduledJob]:
    runners: tuple[tuple[str, bool, IngestionRunner], ...] = (
        ("levelup", config.levelup_configured, run_levelup_ingestion),
        ("skillup", config.skillup_configured, run_skillup_ingestion),
        ("datacamp", config.datacamp_configured, run_datacamp_ingestion),
        ("coursera", config.coursera_configured, run_coursera_ingestion),
        ("linkedin", config.linkedin_configured, run_linkedin_ingestion),
        ("harvard_hmm", config.harvard_hmm_configured, run_harvard_hmm_ingestion),
        (
            "harvard_spark",
            config.harvard_spark_configured,
            run_harvard_spark_ingestion,
        ),
        ("fams", config.fams_configured, run_fams_ingestion),
    )
    return {
        vendor: _bind_scheduled_job(runner, config, store, writer)
        for vendor, configured, runner in runners
        if configured
    }


def _bind_scheduled_job(
    runner: IngestionRunner,
    config: Settings,
    store: CheckpointStore,
    writer: BronzeWriter,
) -> ScheduledJob:
    async def scheduled_ingestion() -> object:
        return await runner(
            config,
            checkpoint_store=store,
            bronze_writer=writer,
        )

    return scheduled_ingestion


def create_app(
    settings: Settings | None = None,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
) -> FastAPI:
    config = settings or get_settings()
    _configure_application_logging(config.log_level)
    store = checkpoint_store or CheckpointStore(config.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(config.bronze_local_path)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
        _configure_application_logging(config.log_level)
        await store.initialize()
        scheduler = None
        if config.scheduler_may_run:
            jobs = _scheduled_jobs(config, store, writer)
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
