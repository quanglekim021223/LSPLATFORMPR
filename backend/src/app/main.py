from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import build_api_router
from app.config.scheduler import ScheduledJob, build_scheduler
from app.core.config import Settings, get_settings
from app.core.logging_config import (
    configure_application_logging as _configure_application_logging,
)
from app.repositories import (
    ADLSGen2BronzeWriter,
    BronzeWriter,
    CheckpointStore,
    LocalBronzeWriter,
)
from app.services.coursera.service import run_coursera_ingestion
from app.services.datacamp.service import run_datacamp_ingestion
from app.services.fams.service import run_fams_ingestion
from app.services.harvard.hmm_service import run_harvard_hmm_ingestion
from app.services.harvard.spark_service import run_harvard_spark_ingestion
from app.services.levelup.service import run_levelup_ingestion
from app.services.linkedin.service import run_linkedin_ingestion
from app.services.skillup.service import run_skillup_ingestion

logger = logging.getLogger(__name__)
IngestionRunner = Callable[..., Awaitable[object]]


def build_bronze_writer(config: Settings) -> BronzeWriter:
    if config.bronze_storage_type == "local":
        return LocalBronzeWriter(config.bronze_local_path)
    return ADLSGen2BronzeWriter(
        account_name=config.adls_account_name,
        file_system=config.adls_file_system,
        base_path=config.adls_base_path,
    )


def build_ingestion_jobs(
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
    writer = bronze_writer or build_bronze_writer(config)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
        _configure_application_logging(config.log_level)
        await store.initialize()
        scheduler = None
        if config.scheduler_may_run:
            jobs = build_ingestion_jobs(config, store, writer)
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

    application.include_router(build_api_router(store))

    return application


app = create_app()
