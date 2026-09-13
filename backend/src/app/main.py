from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from http import HTTPStatus

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

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
from app.services.ingestion_coordinator import IngestionCoordinator
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
    ingestion_jobs: dict[str, ScheduledJob] | None = None,
) -> FastAPI:
    config = settings or get_settings()
    _configure_application_logging(config.log_level)
    store = checkpoint_store or CheckpointStore(config.checkpoint_db_path)
    writer = bronze_writer or build_bronze_writer(config)
    configured_jobs = ingestion_jobs or build_ingestion_jobs(config, store, writer)
    coordinator = IngestionCoordinator(
        configured_jobs,
        progress_reader=store.latest_run,
    )
 
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
        _configure_application_logging(config.log_level)
        config.validate_auth_runtime()
        await store.initialize()
        scheduler = None
        if config.scheduler_may_run:
            if not configured_jobs:
                raise ValueError("Scheduler enabled but no vendor is fully configured")
            scheduler = build_scheduler(config, configured_jobs)
            scheduler.start()
            logger.info(
                "Vendor scheduler started vendors=%s schedule=%s timezone=%s",
                ",".join(configured_jobs),
                config.ingestion_time,
                config.ingestion_timezone,
            )
        application.state.settings = config
        application.state.checkpoint_store = store
        application.state.bronze_writer = writer
        application.state.ingestion_coordinator = coordinator
        application.state.scheduler = scheduler
        try:
            yield
        finally:
            await coordinator.shutdown()
            if scheduler is not None:
                scheduler.shutdown(wait=False)
 
    application = FastAPI(
        title="FSA Learning Vendor Ingestion",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=config.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["DELETE", "GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["Content-Disposition"],
    )

    @application.exception_handler(HTTPException)
    async def http_exception_handler(
        _: Request,
        exc: HTTPException,
    ) -> JSONResponse:
        try:
            message = HTTPStatus(exc.status_code).phrase
        except ValueError:
            message = "HTTP error"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "errcode": exc.status_code,
                "message": message,
                "detail": exc.detail,
            },
            headers=exc.headers,
        )

    def unhandled_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        logger.error(
            "Unhandled request error method=%s path=%s",
            request.method,
            request.url.path,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return JSONResponse(
            status_code=500,
            content={
                "errcode": 500,
                "message": "Internal server error",
                "detail": "An unexpected error occurred. Check application logs.",
            },
        )

    @application.middleware("http")
    async def catch_unhandled_exceptions(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            return unhandled_exception_handler(request, exc)
 
    application.include_router(build_api_router(store, config, writer, coordinator))
    
    return application
 
 
app = create_app()
