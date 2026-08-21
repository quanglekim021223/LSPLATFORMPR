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
            "levelup_base_url": "https://levelup.test",
            "levelup_username": "test-user",
            "levelup_password": "test-password",
            "levelup_api_key": "test-private-key",
            "levelup_page_size": 2,
            "levelup_max_concurrency": 2,
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

