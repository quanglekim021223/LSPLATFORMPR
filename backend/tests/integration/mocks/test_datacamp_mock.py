from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.mocks.app import app as mock_vendor_hub
from app.models import RunStatus
from app.services.datacamp.service import run_datacamp_ingestion
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_server_runs_full_datacamp_pipeline(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        datacamp_base_url="http://mock-vendor-hub/datacamp",
        datacamp_token="mock-datacamp-token",
        datacamp_events_page_size=2,
    )

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=mock_vendor_hub),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog_archived": 1,
        "course_catalog_live": 2,
        "learning_history": 3,
    }
