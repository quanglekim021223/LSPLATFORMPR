from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.handlers.levelup_handler import run_levelup_ingestion
from app.mock_levelup import app as mock_levelup_app
from app.models import RunStatus
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_server_runs_full_ingestion_pipeline(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        levelup_base_url="http://mock-levelup",
        levelup_username="mock-user",
        levelup_password="mock-password",
        levelup_api_key="mock-private-key",
    )

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=mock_levelup_app),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.course_catalog_records == 3
    assert summary.enrollment_records == 3
    assert summary.courses_succeeded == 2
    assert summary.courses_failed == 0
