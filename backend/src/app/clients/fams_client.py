from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.clients.base_client import RetryingHttpClient
from app.core.config import Settings


class FAMSResponseContractError(RuntimeError):
    pass


class FAMSClient:
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
            client,
            settings.http_max_retries,
            sleep=sleep,
            jitter=jitter,
        )

    async def get_training_data(
        self, filters: Mapping[str, str] | None = None
    ) -> tuple[Any, bytes]:
        response = await self.http.request(
            "GET",
            f"{self.settings.fams_base_url.rstrip('/')}/api/fsa-reports/training-data",
            headers={
                "Fsa-Report-Api-Key": self.settings.fams_api_key.get_secret_value(),
                "Accept": "application/json",
            },
            params=filters,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise FAMSResponseContractError(
                "FAMS training-data response is not valid JSON"
            ) from exc
        return payload, response.content

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.fams_secrets()


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 429 or status_code >= 500
    if isinstance(exc, (FAMSResponseContractError, ValueError)):
        return False
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError))
