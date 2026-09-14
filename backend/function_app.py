from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import azure.functions as func

from app.core.config import get_settings
from app.main import build_bronze_writer, build_ingestion_jobs, create_app
from app.models import RunStatus, RunSummary
from app.repositories import CheckpointStore
from app.services.azure_ingestion_backend import AzureIngestionBackend
from app.services.ingestion_coordinator import QueuedIngestion

logger = logging.getLogger(__name__)

settings = get_settings().model_copy(update={"scheduler_enabled": False})
checkpoint_store = CheckpointStore(settings.checkpoint_db_path)
bronze_writer = build_bronze_writer(settings)
storage_connection = settings.azure_web_jobs_storage.get_secret_value()
ingestion_backend = (
    AzureIngestionBackend(
        storage_connection,
        settings.ingestion_queue_name,
        settings.ingestion_status_container,
    )
    if storage_connection
    else None
)
fastapi_app = create_app(
    settings,
    checkpoint_store=checkpoint_store,
    bronze_writer=bronze_writer,
    ingestion_backend=ingestion_backend,
)
ingestion_coordinator = fastapi_app.state.ingestion_coordinator

app = func.AsgiFunctionApp(
    app=fastapi_app,
    http_auth_level=func.AuthLevel.ANONYMOUS,
)


async def run_configured_ingestions() -> None:
    await checkpoint_store.initialize()
    jobs = build_ingestion_jobs(settings, checkpoint_store, bronze_writer)
    if not jobs:
        raise RuntimeError("Azure timer started but no vendor is fully configured")

    logger.info("Azure timer ingestion started vendors=%s", ",".join(jobs))
    semaphore = asyncio.Semaphore(
        settings.fabric_max_concurrent_vendors if settings.fabric_enabled else 8
    )

    async def invoke(job: Callable[[], Awaitable[object]]) -> object:
        async with semaphore:
            return await job()

    results = await asyncio.gather(
        *(invoke(job) for job in jobs.values()),
        return_exceptions=True,
    )
    failed_vendors = [
        vendor
        for vendor, result in zip(jobs, results, strict=True)
        if isinstance(result, BaseException)
        or (isinstance(result, RunSummary) and result.status != RunStatus.SUCCEEDED)
    ]
    if failed_vendors:
        logger.error(
            "Azure timer ingestion failed vendors=%s",
            ",".join(failed_vendors),
        )
        raise RuntimeError(f"Azure timer ingestion failed for: {', '.join(failed_vendors)}")
    logger.info("Azure timer ingestion finished vendors=%s", ",".join(jobs))


@app.timer_trigger(
    schedule="%INGESTION_TIMER_SCHEDULE%",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
async def scheduled_vendor_ingestion(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logger.warning("Azure ingestion timer is past due")
    await run_configured_ingestions()


@app.queue_trigger(
    arg_name="message",
    queue_name="%INGESTION_QUEUE_NAME%",
    connection="AzureWebJobsStorage",
)
async def manual_vendor_ingestion(message: func.QueueMessage) -> None:
    queued = QueuedIngestion.model_validate_json(message.get_body())
    await ingestion_coordinator.run_queued(queued)
