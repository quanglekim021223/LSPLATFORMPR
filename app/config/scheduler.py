from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from app.config.settings import Settings


def build_scheduler(settings: Settings, job: Callable[[], Awaitable[object]]) -> Any:
    # Imports stay here so health endpoints can still start with scheduling disabled
    # when only the API dependencies are installed.
    from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
    from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

    hour, minute = (int(part) for part in settings.ingestion_time.split(":"))
    scheduler = AsyncIOScheduler(timezone=settings.ingestion_timezone)
    scheduler.add_job(
        job,
        trigger=CronTrigger(
            hour=hour,
            minute=minute,
            timezone=settings.ingestion_timezone,
        ),
        id="levelup-daily-ingestion",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    return scheduler
