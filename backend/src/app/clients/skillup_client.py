from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.clients.base_client import RetryingHttpClient
from app.core.config import Settings


class SkillUpResponseContractError(RuntimeError):
    pass


class SkillUpClient:
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
        self, base_url: str, path: str, params: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bytes]:
        response = await self.http.request(
            "GET",
            f"{base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={"x-api-key": self.settings.skillup_api_key.get_secret_value()},
            params=params,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise SkillUpResponseContractError(
                f"Expected a JSON object from {path}"
            ) from exc
        if not isinstance(payload, dict):
            raise SkillUpResponseContractError(f"Expected a JSON object from {path}")
        return payload, response.content

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.skillup_secrets()


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 429 or status_code >= 500
    if isinstance(exc, (SkillUpResponseContractError, ValueError)):
        return False
    return True
