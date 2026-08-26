from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.mocks.app import app as mock_vendor_hub
from app.models import RunStatus
from app.services.coursera.service import run_coursera_ingestion
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_server_runs_full_coursera_pipeline(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        coursera_token_url=(
            "http://mock-vendor-hub/coursera/oauth2/client_credentials/token"
        ),
        coursera_base_url="http://mock-vendor-hub/coursera",
        coursera_username="mock-coursera-user",
        coursera_password="mock-coursera-password",
        coursera_org_id="mock-org",
        coursera_content_detail_path_template=(
            "/{org_id}/contents/{content_id}/detail"
        ),
        coursera_page_size=2,
    )

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=mock_vendor_hub),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog": 3,
        "course_detail": 3,
        "learning_history": 3,
    }
