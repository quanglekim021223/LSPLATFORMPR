from __future__ import annotations

from collections.abc import Callable

from app.config.scheduler import build_scheduler


def test_scheduler_defaults_to_single_daily_0500_job(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()

    async def job() -> object:
        return None

    scheduler = build_scheduler(
        settings,
        {"levelup": job, "skillup": job, "datacamp": job, "coursera": job},  # type: ignore[arg-type]
    )
    for vendor in ("levelup", "skillup", "datacamp", "coursera"):
        scheduled_job = scheduler.get_job(f"{vendor}-daily-ingestion")
        assert scheduled_job is not None
        assert scheduled_job.max_instances == 1
        assert scheduled_job.coalesce is True
        assert "hour='5'" in str(scheduled_job.trigger)
        assert "minute='0'" in str(scheduled_job.trigger)
