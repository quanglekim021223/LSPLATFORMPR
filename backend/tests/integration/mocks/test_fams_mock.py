from __future__ import annotations

import httpx
import pytest

from app.mocks.app import app


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
