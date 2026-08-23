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
