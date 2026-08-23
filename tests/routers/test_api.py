from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.main import create_app
from app.models import RunStatus
from app.repositories.checkpoint_repository import CheckpointStore


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
            missing_skillup = await client.get("/jobs/skillup/latest")
            missing_datacamp = await client.get("/jobs/datacamp/latest")
            missing_coursera = await client.get("/jobs/coursera/latest")
            missing_linkedin = await client.get("/jobs/linkedin/latest")
            missing_hmm = await client.get("/jobs/harvard-hmm/latest")
            missing_spark = await client.get("/jobs/harvard-spark/latest")
            assert health.json() == {"status": "ok"}
            assert ready.json() == {"status": "ready"}
            assert missing.status_code == 404
            assert missing_skillup.status_code == 404
            assert missing_datacamp.status_code == 404
            assert missing_coursera.status_code == 404
            assert missing_linkedin.status_code == 404
            assert missing_hmm.status_code == 404
            assert missing_spark.status_code == 404

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

            skillup_run_id = "22222222-2222-4222-8222-222222222222"
            await store.start_run(skillup_run_id, "skillup")
            await store.record_completed_page(
                skillup_run_id, "skill_taxonomy", 1, 3
            )
            await store.finish_run(skillup_run_id, RunStatus.SUCCEEDED)

            latest_skillup = await client.get("/jobs/skillup/latest")
            assert latest_skillup.status_code == 200
            assert latest_skillup.json()["run_id"] == skillup_run_id
            assert latest_skillup.json()["vendor"] == "skillup"
            assert latest_skillup.json()["records_by_domain"] == {
                "skill_taxonomy": 3
            }

            datacamp_run_id = "33333333-3333-4333-8333-333333333333"
            await store.start_run(datacamp_run_id, "datacamp")
            await store.record_completed_page(
                datacamp_run_id, "learning_history", 1, 4
            )
            await store.finish_run(datacamp_run_id, RunStatus.SUCCEEDED)

            latest_datacamp = await client.get("/jobs/datacamp/latest")
            assert latest_datacamp.status_code == 200
            assert latest_datacamp.json()["run_id"] == datacamp_run_id
            assert latest_datacamp.json()["vendor"] == "datacamp"
            assert latest_datacamp.json()["records_by_domain"] == {
                "learning_history": 4
            }

            coursera_run_id = "44444444-4444-4444-8444-444444444444"
            await store.start_run(coursera_run_id, "coursera")
            await store.record_completed_page(
                coursera_run_id, "course_catalog", 0, 5
            )
            await store.finish_run(coursera_run_id, RunStatus.SUCCEEDED)

            latest_coursera = await client.get("/jobs/coursera/latest")
            assert latest_coursera.status_code == 200
            assert latest_coursera.json()["run_id"] == coursera_run_id
            assert latest_coursera.json()["vendor"] == "coursera"
            assert latest_coursera.json()["records_by_domain"] == {
                "course_catalog": 5
            }

            linkedin_run_id = "55555555-5555-4555-8555-555555555555"
            await store.start_run(linkedin_run_id, "linkedin")
            await store.record_completed_page(
                linkedin_run_id, "learning_history", 1, 6
            )
            await store.finish_run(linkedin_run_id, RunStatus.SUCCEEDED)

            latest_linkedin = await client.get("/jobs/linkedin/latest")
            assert latest_linkedin.status_code == 200
            assert latest_linkedin.json()["run_id"] == linkedin_run_id
            assert latest_linkedin.json()["vendor"] == "linkedin"
            assert latest_linkedin.json()["records_by_domain"] == {
                "learning_history": 6
            }

            hmm_run_id = "66666666-6666-4666-8666-666666666666"
            await store.start_run(hmm_run_id, "harvard_hmm")
            await store.record_completed_page(hmm_run_id, "course_catalog", 0, 2)
            await store.finish_run(hmm_run_id, RunStatus.SUCCEEDED)
            latest_hmm = await client.get("/jobs/harvard-hmm/latest")
            assert latest_hmm.status_code == 200
            assert latest_hmm.json()["vendor"] == "harvard_hmm"

            spark_run_id = "77777777-7777-4777-8777-777777777777"
            await store.start_run(spark_run_id, "harvard_spark")
            await store.record_completed_page(
                spark_run_id, "learning_history", 1, 0
            )
            await store.finish_run(spark_run_id, RunStatus.SUCCEEDED)
            latest_spark = await client.get("/jobs/harvard-spark/latest")
            assert latest_spark.status_code == 200
            assert latest_spark.json()["vendor"] == "harvard_spark"
