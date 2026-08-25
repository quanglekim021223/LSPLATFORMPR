from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.clients.base_client import RetryingHttpClient
from app.core.config import Settings


class ResponseContractError(RuntimeError):
    pass


class LevelUpClient:
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
        self._token: str | None = None
        self._refresh_lock = asyncio.Lock()

    async def authenticate(self) -> str:
        api_key = self.settings.levelup_api_key.get_secret_value()
        response = await self.http.request(
            "POST",
            self._url(self.settings.levelup_auth_path),
            headers={
                "X-API-Key": api_key,
                "x-api-version": self.settings.levelup_api_version,
                "Content-Type": "application/json",
            },
            json={
                "username": self.settings.levelup_username.get_secret_value(),
                "password": self.settings.levelup_password.get_secret_value(),
                "privateKey": api_key,
            },
        )
        response.raise_for_status()
        token = self._extract_token(response)
        self._token = token
        return token

    async def get_json(
        self, path: str, params: Mapping[str, Any]
    ) -> tuple[Any, bytes]:
        if self._token is None:
            await self.authenticate()
        token_used = self._token
        response = await self._authorized_get(path, params, token_used)
        if response.status_code == 401:
            async with self._refresh_lock:
                if self._token == token_used:
                    await self.authenticate()
                refreshed_token = self._token
            response = await self._authorized_get(path, params, refreshed_token)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise ResponseContractError(f"Expected a JSON object from {path}") from exc
        return payload, response.content

    async def _authorized_get(
        self, path: str, params: Mapping[str, Any], token: str | None
    ) -> httpx.Response:
        if not token:
            raise ResponseContractError("LevelUP authentication returned an empty token")
        return await self.http.request(
            "GET",
            self._url(path),
            headers={
                "Authorization": token,
                "X-API-Key": self.settings.levelup_api_key.get_secret_value(),
                "x-api-version": self.settings.levelup_api_version,
            },
            params=params,
        )

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.levelup_secrets() + ((self._token,) if self._token else ())

    def _url(self, path: str) -> str:
        return f"{self.settings.levelup_base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _extract_token(response: httpx.Response) -> str:
        try:
            value = response.json()
        except ValueError:
            value = response.text
        token: object
        if isinstance(value, str):
            token = value
        elif isinstance(value, dict):
            token = value.get("token") or value.get("access_token")
        else:
            token = None
        if not isinstance(token, str) or not token.strip():
            raise ResponseContractError("Authentication response did not contain a token")
        return token.strip()


def is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 429 or status_code >= 500
    if isinstance(exc, (ResponseContractError, ValueError)):
        return False
    return True
