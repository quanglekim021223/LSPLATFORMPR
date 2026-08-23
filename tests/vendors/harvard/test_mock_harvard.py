from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.config import Settings
from app.mocks.app import app as mock_app
from app.vendors.harvard.catalog_client import HarvardCatalogClient
from app.vendors.harvard.models import vendor_config
from tests.conftest import no_sleep


@pytest.mark.parametrize(
    ("vendor", "client_id", "client_secret", "org_key", "catalog_code", "count"),
    [
        (
            "harvard_hmm",
            "mock-hmm-client",
            "mock-hmm-secret",
            "mock-hmm-org",
            "HMM",
            3,
        ),
        (
            "harvard_spark",
            "mock-spark-client",
            "mock-spark-secret",
            "mock-spark-org",
            "HBR_SPARK",
            2,
        ),
    ],
)
@pytest.mark.asyncio
async def test_mock_catalog_authentication_and_catalog(
    settings_factory: Callable[..., Settings],
    vendor: str,
    client_id: str,
    client_secret: str,
    org_key: str,
    catalog_code: str,
    count: int,
) -> None:
    overrides = {
        "harvard_catalog_base_url": "http://mock/harvard/v1",
        f"{vendor}_client_id": client_id,
        f"{vendor}_client_secret": client_secret,
        f"{vendor}_org_key": org_key,
    }
    settings = settings_factory(**overrides)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app), base_url="http://mock"
    ) as http:
        client = HarvardCatalogClient(
            settings, vendor_config(settings, vendor), http, sleep=no_sleep
        )
        payload, _ = await client.get_json(
            f"/api/catalog/{org_key}",
            {"catalogs": catalog_code, "start": 0, "limit": 1000},
        )
    assert payload["count"] == count
    assert len(payload["list"]) == count
