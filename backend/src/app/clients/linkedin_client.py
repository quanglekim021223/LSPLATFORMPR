from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import parse_qsl, quote_plus

import httpx

from app.clients.base_client import RetryingHttpClient
from app.core.config import Settings

logger = logging.getLogger(__name__)
URN_PLACEHOLDER = "{urn}"


class LinkedInResponseContractError(RuntimeError):
    pass


class LinkedInClient:
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
        response = await self.http.request(
            "POST",
            self.settings.linkedin_token_url,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
                "client_id": self.settings.linkedin_client_id.get_secret_value(),
                "client_secret": (
                    self.settings.linkedin_client_secret.get_secret_value()
                ),
            },
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise LinkedInResponseContractError(
                "LinkedIn token response must be valid JSON"
            ) from exc
        from app.schemas.linkedin import extra_field_paths, validate_token

        contract = validate_token(payload)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "LinkedIn Token contains new contract fields=%s",
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
            raise LinkedInResponseContractError(
                f"Expected a JSON object from {path}"
            ) from exc
        if not isinstance(payload, dict):
            raise LinkedInResponseContractError(f"Expected a JSON object from {path}")
        return payload, response.content

    def asset_detail_params(self, urn: str) -> dict[str, str]:
        template = self.settings.linkedin_asset_detail_query_template
        if URN_PLACEHOLDER not in template or template.count(URN_PLACEHOLDER) != 1:
            raise ValueError(
                "LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE must contain {urn} exactly once"
            )
        remaining = template.replace(URN_PLACEHOLDER, "")
        if "{" in remaining or "}" in remaining:
            raise ValueError(
                "LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE only supports {urn}"
            )
        query = template.lstrip("?").replace(URN_PLACEHOLDER, quote_plus(urn))
        pairs = parse_qsl(query, keep_blank_values=True)
        if not pairs or len({key for key, _ in pairs}) != len(pairs):
            raise ValueError(
                "LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE must be a unique query string"
            )
        return dict(pairs)

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.linkedin_secrets() + (
            (self._token,) if self._token else ()
        )

    async def _authorized_get(
        self, path: str, params: Mapping[str, Any], token: str | None
    ) -> httpx.Response:
        if not token:
            raise LinkedInResponseContractError(
                "LinkedIn authentication returned an empty token"
            )
        return await self.http.request(
            "GET",
            f"{self.settings.linkedin_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            params=params,
        )


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 408 or status_code == 429 or status_code >= 500
    if isinstance(exc, (LinkedInResponseContractError, ValueError)):
        return False
    return True
