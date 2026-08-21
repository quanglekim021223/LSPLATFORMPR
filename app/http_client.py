from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)


class ResponseContractError(RuntimeError):
    pass


class RetryingHttpClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        max_retries: int,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.client = client
        self.max_retries = max_retries
        self.sleep = sleep
        self.jitter = jitter

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        endpoint = urlsplit(url).path or "/"
        for retry_number in range(self.max_retries + 1):
            attempt = retry_number + 1
            try:
                response = await self.client.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if retry_number >= self.max_retries:
                    raise
                wait_seconds = self._backoff_seconds(retry_number)
                logger.warning(
                    "Retrying request attempt=%d endpoint=%s error=%s wait_seconds=%.3f",
                    attempt,
                    endpoint,
                    type(exc).__name__,
                    wait_seconds,
                )
                await self.sleep(wait_seconds)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if retry_number < self.max_retries:
                    retry_after = self._retry_after_seconds(response.headers)
                    wait_seconds = (
                        retry_after
                        if retry_after is not None
                        else self._backoff_seconds(retry_number)
                    )
                    logger.warning(
                        "Retrying request attempt=%d endpoint=%s status_code=%d "
                        "wait_seconds=%.3f",
                        attempt,
                        endpoint,
                        response.status_code,
                        wait_seconds,
                    )
                    await self.sleep(wait_seconds)
                    continue
            return response
        raise AssertionError("retry loop exhausted unexpectedly")

    def _backoff_seconds(self, retry_number: int) -> float:
        return float(min(60.0, (2**retry_number) + self.jitter()))

    @staticmethod
    def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
        value = headers.get("Retry-After")
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


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

    async def get_json(self, path: str, params: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
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
        if not isinstance(payload, dict):
            raise ResponseContractError(f"Expected a JSON object from {path}")
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
