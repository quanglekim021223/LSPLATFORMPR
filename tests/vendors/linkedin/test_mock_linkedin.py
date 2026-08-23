from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.handlers.linkedin_handler import run_linkedin_ingestion
from app.mocks.app import app as mock_vendor_hub
from app.models import RunStatus
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_server_runs_full_linkedin_pipeline(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        linkedin_token_url="http://mock-vendor-hub/linkedin/oauth/v2/accessToken",
        linkedin_base_url="http://mock-vendor-hub/linkedin",
        linkedin_client_id="mock-linkedin-client",
        linkedin_client_secret="mock-linkedin-secret",
        linkedin_page_size=2,
        linkedin_history_start_time=(
            datetime.now(UTC) - timedelta(hours=1)
        ).isoformat(),
        linkedin_asset_detail_query_template=(
            "q=criteria&assetFilteringCriteria.urn={urn}"
        ),
    )

    summary = await run_linkedin_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=mock_vendor_hub),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog": 3,
        "course_detail": 3,
        "learning_history": 1,
    }
