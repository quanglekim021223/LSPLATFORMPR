from datetime import UTC, datetime, timedelta

import pytest

from app.repositories import CheckpointStore
from app.services.coursera.service import CourseraJob
from app.services.datacamp.service import DataCampJob
from app.services.linkedin.service import LinkedInJob
from app.services.skillup.service import SkillUpJob


@pytest.mark.parametrize("vendor", ["skillup", "datacamp", "linkedin", "coursera"])
async def test_fabric_daily_range_does_not_full_pull_after_month_boundary(settings_factory, vendor):
    settings = settings_factory(
        history_periodic_resync_enabled=False,
        linkedin_history_start_time="2000-01-01T00:00:00Z",
    )
    store = CheckpointStore(settings.checkpoint_db_path)
    await store.initialize()
    now = datetime.now(UTC)
    anchor = now - timedelta(days=1)
    domain = "assessment_history" if vendor == "skillup" else "learning_history"

    def encode(value):
        if vendor in {"linkedin", "coursera"}:
            return str(int(value.timestamp() * 1000))
        return value.isoformat().replace("+00:00", "Z")

    await store.set_watermark(vendor, domain, encode(now - timedelta(days=400)), "old", "full_sync")
    await store.set_watermark(vendor, domain, encode(anchor), "recent", "daily_sync")
    jobs = {
        "skillup": SkillUpJob,
        "datacamp": DataCampJob,
        "linkedin": LinkedInJob,
        "coursera": CourseraJob,
    }
    job = jobs[vendor](settings, None, store, None)
    if vendor == "skillup":
        plan = await job._assessment_range(None, None)
        start = datetime.fromisoformat(plan[0].replace("Z", "+00:00"))
    elif vendor == "datacamp":
        plan = await job._history_range(None, None)
        start = datetime.fromisoformat(plan[0].replace("Z", "+00:00"))
    elif vendor == "linkedin":
        plan = await job._history_sync_plan(now)
        start = plan[0]
    else:
        plan = await job._history_sync_plan(now)
        start = datetime.fromtimestamp(plan[0] / 1000, UTC)
    assert abs((start - (anchor - timedelta(days=3))).total_seconds()) < 1
    assert plan[-1] is None  # no full-sync watermark advancement
