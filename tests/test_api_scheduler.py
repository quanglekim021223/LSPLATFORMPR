from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.checkpoint import CheckpointStore
from app.main import create_app
from app.models import RunStatus
from app.scheduler import build_scheduler


@pytest.mark.asyncio
async def test_health_ready_latest_and_scheduler_disabled_in_test(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory(scheduler_enabled=True, app_env="test")
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    app = create_app(settings, checkpoint_store=store)  # type: ignore[arg-type]

    async with app.router.lifespan_context(app):
        assert app.state.scheduler is None
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            health = await client.get("/health")
            ready = await client.get("/ready")
            missing = await client.get("/jobs/levelup/latest")
            assert health.json() == {"status": "ok"}
            assert ready.json() == {"status": "ready"}
            assert missing.status_code == 404

            run_id = "11111111-1111-4111-8111-111111111111"
            await store.start_run(run_id, "levelup")
            await store.record_completed_page(run_id, "course_catalog", 0, 2)
            await store.add_courses(run_id, ["c1"])
            await store.mark_course(run_id, "c1", "completed")
            await store.finish_run(run_id, RunStatus.SUCCEEDED)

            latest = await client.get("/jobs/levelup/latest")
            assert latest.status_code == 200
            assert latest.json()["run_id"] == run_id
            assert latest.json()["status"] == "succeeded"
            assert latest.json()["course_catalog_records"] == 2
            assert "token" not in latest.text.casefold()


def test_scheduler_defaults_to_single_daily_0500_job(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()

    async def job() -> object:
        return None

    scheduler = build_scheduler(settings, job)  # type: ignore[arg-type]
    scheduled_job = scheduler.get_job("levelup-daily-ingestion")
    assert scheduled_job is not None
    assert scheduled_job.max_instances == 1
    assert scheduled_job.coalesce is True
    assert "hour='5'" in str(scheduled_job.trigger)
    assert "minute='0'" in str(scheduled_job.trigger)

