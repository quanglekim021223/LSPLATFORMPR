from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.mocks.app import app
from app.models import RunStatus
from app.services.fams.service import run_fams_ingestion
from tests.conftest import no_sleep


@pytest.mark.asyncio
async def test_mock_fams_full_filtered_and_api_key() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://mock",
    ) as client:
        denied = await client.get("/fams/api/fsa-reports/training-data")
        assert denied.status_code == 401

        headers = {
            "Fsa-Report-Api-Key": "mock-fams-key",
            "Accept": "application/json",
        }
        full = await client.get(
            "/fams/api/fsa-reports/training-data",
            headers=headers,
        )
        assert full.status_code == 200
        assert len(full.json()["data"]["classList"]) == 2
        assert len(full.json()["data"]["studentList"]) == 3

        filtered = await client.get(
            "/fams/api/fsa-reports/training-data",
            headers=headers,
            params={"status": "CLOSED", "site": "HCM"},
        )
        assert filtered.status_code == 200
        assert len(filtered.json()["data"]["classList"]) == 1
        assert len(filtered.json()["data"]["studentList"]) == 2


@pytest.mark.asyncio
async def test_mock_server_skips_unchanged_fams_response(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        fams_base_url="http://mock-vendor-hub/fams",
        fams_token="mock-fams-key",
        fams_load_mode="full",
    )

    first = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=app),
        sleep=no_sleep,
    )
    second = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.ASGITransport(app=app),
        sleep=no_sleep,
    )

    assert first.status == RunStatus.SUCCEEDED
    assert first.records_by_domain == {"training_data": 5}
    assert second.status == RunStatus.SUCCEEDED
    assert second.records_by_domain == {"training_data": 0}
