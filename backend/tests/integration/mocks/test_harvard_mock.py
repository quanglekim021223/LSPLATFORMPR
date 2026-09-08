from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from app.clients.harvard_catalog_client import HarvardCatalogClient
from app.core.config import Settings
from app.mocks.app import app as mock_app
from app.mocks.settings import get_mock_settings
from app.models import RunStatus
from app.models.harvard import vendor_config
from app.services.harvard.hmm_service import run_harvard_hmm_ingestion
from app.services.harvard.spark_service import run_harvard_spark_ingestion
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


@pytest.mark.parametrize(
    ("vendor", "runner", "catalog_records"),
    [
        ("harvard_hmm", run_harvard_hmm_ingestion, 3),
        ("harvard_spark", run_harvard_spark_ingestion, 2),
    ],
)
@pytest.mark.asyncio
async def test_mock_server_skips_unchanged_harvard_data(
    settings_factory: Callable[..., Settings],
    tmp_path: Path,
    vendor: str,
    runner: Callable[..., Awaitable[Any]],
    catalog_records: int,
) -> None:
    mock = get_mock_settings()
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text(
        f"{mock.mock_harvard_sftp_host} {mock.mock_harvard_sftp_host_key}\n",
        encoding="utf-8",
    )
    settings = settings_factory(
        harvard_catalog_base_url="http://mock/harvard/v1",
        harvard_hmm_client_id="mock-hmm-client",
        harvard_hmm_client_secret="mock-hmm-secret",
        harvard_hmm_org_key="mock-hmm-org",
        harvard_spark_client_id="mock-spark-client",
        harvard_spark_client_secret="mock-spark-secret",
        harvard_spark_org_key="mock-spark-org",
        harvard_sftp_mock_enabled=True,
        harvard_sftp_host=mock.mock_harvard_sftp_host,
        harvard_sftp_username=mock.mock_harvard_sftp_username.get_secret_value(),
        harvard_sftp_password=mock.mock_harvard_sftp_password.get_secret_value(),
        harvard_sftp_known_hosts=known_hosts,
    )
    now = datetime(2026, 9, 7, 12, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))

    first = await runner(
        settings,
        transport=httpx.ASGITransport(app=mock_app),
        sleep=no_sleep,
        now=lambda: now,
    )
    second = await runner(
        settings,
        transport=httpx.ASGITransport(app=mock_app),
        sleep=no_sleep,
        now=lambda: now,
    )

    assert first.status == RunStatus.SUCCEEDED
    assert first.records_by_domain == {
        "course_catalog": catalog_records,
        "learning_history": 1,
    }
    assert second.status == RunStatus.SUCCEEDED
    assert sum(second.records_by_domain.values()) == 0
