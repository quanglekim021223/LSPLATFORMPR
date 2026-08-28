from __future__ import annotations

import asyncio
import logging

import azure.functions as func

from app.core.config import get_settings
from app.main import build_bronze_writer, build_ingestion_jobs, create_app
from app.repositories import CheckpointStore

logger = logging.getLogger(__name__)

settings = get_settings().model_copy(update={"scheduler_enabled": False})
checkpoint_store = CheckpointStore(settings.checkpoint_db_path)
bronze_writer = build_bronze_writer(settings)
fastapi_app = create_app(
    settings,
    checkpoint_store=checkpoint_store,
    bronze_writer=bronze_writer,
)

app = func.AsgiFunctionApp(
    app=fastapi_app,
    http_auth_level=func.AuthLevel.FUNCTION,
)


async def run_configured_ingestions() -> None:
    await checkpoint_store.initialize()
    jobs = build_ingestion_jobs(settings, checkpoint_store, bronze_writer)
    if not jobs:
        raise RuntimeError("Azure timer started but no vendor is fully configured")

    logger.info("Azure timer ingestion started vendors=%s", ",".join(jobs))
    results = await asyncio.gather(
        *(job() for job in jobs.values()),
        return_exceptions=True,
    )
    failed_vendors = [
        vendor
        for vendor, result in zip(jobs, results, strict=True)
        if isinstance(result, BaseException)
    ]
    if failed_vendors:
        logger.error(
            "Azure timer ingestion failed vendors=%s",
            ",".join(failed_vendors),
        )
        raise RuntimeError(
            f"Azure timer ingestion failed for: {', '.join(failed_vendors)}"
        )
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
