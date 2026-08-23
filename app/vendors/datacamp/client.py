from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.config import Settings
from app.helpers.http_client import RetryingHttpClient


class DataCampResponseContractError(RuntimeError):
    pass


class DataCampClient:
    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self.http = RetryingHttpClient(
            client, settings.http_max_retries, sleep=sleep, jitter=jitter
        )

    async def get_json(
        self, path: str, params: Mapping[str, Any] | None = None
    ) -> tuple[Any, bytes]:
        response = await self.http.request(
            "GET",
            f"{self.settings.datacamp_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": (
                    f"Bearer {self.settings.datacamp_token.get_secret_value()}"
                ),
                "Accept": "application/json",
            },
            params=params,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise DataCampResponseContractError(
                f"Expected valid JSON from {path}"
            ) from exc
        return payload, response.content

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.datacamp_secrets()


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 429 or status_code >= 500
    if isinstance(exc, (DataCampResponseContractError, ValueError)):
        return False
    return True
