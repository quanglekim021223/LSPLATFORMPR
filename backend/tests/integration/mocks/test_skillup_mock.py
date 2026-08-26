from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.mocks.app import app as mock_vendor_hub
from app.models import RunStatus
from app.services.skillup.service import run_skillup_ingestion
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_server_runs_full_skillup_pipeline(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        skillup_intelligence_base_url="http://mock-vendor-hub/skillup",
        skillup_reports_base_url="http://mock-vendor-hub/skillup",
        skillup_api_key="mock-skillup-key",
        skillup_page_size=2,
    )

    summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=mock_vendor_hub),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "assessment_history": 3,
        "skill_inventory": 3,
        "skill_taxonomy": 3,
    }


@pytest.mark.asyncio
async def test_mock_server_rejects_wrong_api_key() -> None:
    transport = httpx.ASGITransport(app=mock_vendor_hub)
    async with httpx.AsyncClient(transport=transport, base_url="http://mock") as client:
        response = await client.get(
            "/skillup/taxonomy",
            params={"PageNumber": 1, "PageSize": 2},
            headers={"x-api-key": "wrong"},
        )

    assert response.status_code == 401
