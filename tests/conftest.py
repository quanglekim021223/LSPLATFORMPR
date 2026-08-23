from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.config import Settings


@pytest.fixture
def settings_factory(tmp_path: Path) -> Callable[..., Settings]:
    def factory(**overrides: Any) -> Settings:
        values: dict[str, Any] = {
            "app_env": "test",
            "scheduler_enabled": False,
            "ingestion_time": "05:00",
            "ingestion_timezone": "Asia/Ho_Chi_Minh",
            "levelup_base_url": "https://levelup.test",
            "levelup_username": "test-user",
            "levelup_password": "test-password",
            "levelup_api_key": "test-private-key",
            "levelup_page_size": 2,
            "levelup_max_concurrency": 2,
            "skillup_intelligence_base_url": "https://skillup-intelligence.test",
            "skillup_reports_base_url": "https://skillup-reports.test",
            "skillup_api_key": "test-skillup-key",
            "skillup_page_size": 2,
            "skillup_assessment_start_date": "2000-01-01T00:00:00Z",
            "datacamp_base_url": "https://datacamp.test",
            "datacamp_token": "test-datacamp-token",
            "datacamp_events_page_size": 2,
            "coursera_token_url": "https://coursera-auth.test/oauth2/client_credentials/token",
            "coursera_base_url": "https://coursera.test",
            "coursera_username": "test-coursera-user",
            "coursera_password": "test-coursera-password",
            "coursera_org_id": "test-org",
            "coursera_content_detail_path_template": (
                "/{org_id}/contents/{content_id}/detail"
            ),
            "coursera_page_size": 2,
            "coursera_max_concurrency": 2,
            "linkedin_token_url": "https://linkedin.test/oauth/v2/accessToken",
            "linkedin_base_url": "https://linkedin.test/v2",
            "linkedin_client_id": "test-linkedin-client",
            "linkedin_client_secret": "test-linkedin-secret",
            "linkedin_page_size": 2,
            "linkedin_history_start_time": "2026-08-01T00:00:00Z",
            "linkedin_max_concurrency": 2,
            "linkedin_asset_detail_query_template": (
                "q=criteria&assetFilteringCriteria.urn={urn}"
            ),
            "harvard_catalog_base_url": "https://harvard.test/v1",
            "harvard_page_size": 2,
            "harvard_hmm_client_id": "test-hmm-client",
            "harvard_hmm_client_secret": "test-hmm-secret",
            "harvard_hmm_org_key": "test-hmm-org",
            "harvard_spark_client_id": "test-spark-client",
            "harvard_spark_client_secret": "test-spark-secret",
            "harvard_spark_org_key": "test-spark-org",
            "harvard_sftp_host": "sftp.harvard.test",
            "harvard_sftp_username": "test-sftp-user",
            "harvard_sftp_password": "test-sftp-password",
            "harvard_sftp_remote_dir": "/reports",
            "harvard_sftp_known_hosts": tmp_path / "known_hosts",
            "harvard_report_date_offset_days": 1,
            "harvard_sftp_poll_interval_seconds": 300,
            "harvard_sftp_max_wait_seconds": 7200,
            "harvard_sftp_max_retries": 3,
            "http_max_retries": 0,
            "bronze_local_path": tmp_path / "bronze",
            "checkpoint_db_path": tmp_path / "state" / "ingestion.db",
        }
        values.update(overrides)
        return Settings(**values)

    return factory


async def no_sleep(_seconds: float) -> None:
    return None


def response(request: httpx.Request, status: int, payload: object) -> httpx.Response:
    return httpx.Response(status, json=payload, request=request)


Handler = Callable[[httpx.Request], httpx.Response | Awaitable[httpx.Response]]
