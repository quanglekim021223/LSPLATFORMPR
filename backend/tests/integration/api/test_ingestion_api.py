from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from app.auth.auth import create_access_token
from app.main import create_app
from app.models import RunStatus, RunSummary


@pytest.mark.asyncio
async def test_starts_tracks_and_prevents_duplicate_ingestion(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    release = asyncio.Event()

    async def levelup() -> RunSummary:
        await release.wait()
        return RunSummary(
            run_id="tracked-levelup-run",
            vendor="levelup",
            status=RunStatus.SUCCEEDED,
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
            records_by_domain={"course_catalog": 3},
        )

    app = create_app(settings, ingestion_jobs={"levelup": levelup})  # type: ignore[arg-type]
    token = create_access_token(
        user_id=settings.auth_admin_username,  # type: ignore[attr-defined]
        secret_key=settings.auth_jwt_secret.get_secret_value(),  # type: ignore[attr-defined]
        expire_minutes=10,
    )
    headers = {"Authorization": f"Bearer {token}"}

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers=headers,
        ) as client:
            started = await client.post("/ingestions", json={"vendors": ["levelup"]})
            assert started.status_code == 202
            job_id = started.json()["job_id"]

            duplicate = await client.post("/ingestions", json={"vendors": ["levelup"]})
            assert duplicate.status_code == 409
            assert duplicate.json()["detail"]["job_id"] == job_id

            release.set()
            for _ in range(100):
                tracked = await client.get(f"/ingestions/{job_id}")
                if tracked.json()["status"] == "succeeded":
                    break
                await asyncio.sleep(0)
            assert tracked.json()["vendor_runs"][0]["run_id"] == "tracked-levelup-run"
            assert tracked.json()["total_records"] == 3
