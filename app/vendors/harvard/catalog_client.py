from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.config import Settings
from app.helpers.http_client import RetryingHttpClient
from app.vendors.harvard.models import (
    HarvardResponseContractError,
    HarvardVendorConfig,
    extra_field_paths,
    validate_token,
)

logger = logging.getLogger(__name__)


class HarvardCatalogContractError(HarvardResponseContractError):
    pass


class HarvardCatalogClient:
    def __init__(
        self,
        settings: Settings,
        vendor: HarvardVendorConfig,
        client: httpx.AsyncClient,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self.vendor = vendor
        self.http = RetryingHttpClient(
            client, settings.http_max_retries, sleep=sleep, jitter=jitter
        )
        self._token: str | None = None
        self._refresh_lock = asyncio.Lock()

    async def authenticate(self) -> str:
        response = await self.http.request(
            "POST",
            self._url("/api/oauth/v2/accesstoken"),
            auth=httpx.BasicAuth(self.vendor.client_id, self.vendor.client_secret),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
                "scope": "hbp.org.api/catalog.read",
            },
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise HarvardCatalogContractError(
                "Harvard token response must be valid JSON"
            ) from exc
        contract = validate_token(payload)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "Harvard Token contains new contract fields=%s",
                ",".join(extras),
            )
        self._token = contract.access_token.strip()
        return self._token

    async def get_json(
        self, path: str, params: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bytes]:
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
            raise HarvardCatalogContractError(
                f"Expected a JSON object from {path}"
            ) from exc
        if not isinstance(payload, dict):
            raise HarvardCatalogContractError(f"Expected a JSON object from {path}")
        return payload, response.content

    def sensitive_values(self) -> tuple[str, ...]:
        return self.vendor.sensitive_values() + (
            (self._token,) if self._token else ()
        )

    async def _authorized_get(
        self, path: str, params: Mapping[str, Any], token: str | None
    ) -> httpx.Response:
        if not token:
            raise HarvardCatalogContractError(
                "Harvard authentication returned an empty token"
            )
        return await self.http.request(
            "GET",
            self._url(path),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            params=params,
        )

    def _url(self, path: str) -> str:
        return f"{self.settings.harvard_catalog_base_url.rstrip('/')}/{path.lstrip('/')}"


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 429 or status_code >= 500
    if isinstance(
        exc, (HarvardCatalogContractError, HarvardResponseContractError, ValueError)
    ):
        return False
    return True
