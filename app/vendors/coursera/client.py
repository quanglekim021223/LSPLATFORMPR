from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from string import Formatter
from typing import Any
from urllib.parse import quote

import httpx

from app.config import Settings
from app.helpers.http_client import RetryingHttpClient


class CourseraResponseContractError(RuntimeError):
    pass


class CourseraClient:
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
            self.settings.coursera_token_url,
            auth=httpx.BasicAuth(
                self.settings.coursera_username.get_secret_value(),
                self.settings.coursera_password.get_secret_value(),
            ),
            headers={"Accept": "application/json"},
            data={"grant_type": "client_credentials"},
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise CourseraResponseContractError(
                "Coursera token response must be valid JSON"
            ) from exc
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not token.strip():
            raise CourseraResponseContractError(
                "Coursera token response did not contain access_token"
            )
        self._token = token.strip()
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
            raise CourseraResponseContractError(
                f"Expected a JSON object from {path}"
            ) from exc
        if not isinstance(payload, dict):
            raise CourseraResponseContractError(f"Expected a JSON object from {path}")
        return payload, response.content

    def content_detail_path(self, content_id: str) -> str:
        template = self.settings.coursera_content_detail_path_template
        fields = {
            field_name
            for _, field_name, format_spec, conversion in Formatter().parse(template)
            if field_name is not None
            and not format_spec
            and conversion is None
        }
        parsed_fields = {
            field_name
            for _, field_name, _, _ in Formatter().parse(template)
            if field_name is not None
        }
        allowed = {"org_id", "content_id"}
        if parsed_fields != fields or not parsed_fields <= allowed:
            raise ValueError(
                "COURSERA_CONTENT_DETAIL_PATH_TEMPLATE only supports "
                "{org_id} and {content_id}"
            )
        if "content_id" not in parsed_fields:
            raise ValueError(
                "COURSERA_CONTENT_DETAIL_PATH_TEMPLATE must contain {content_id}"
            )
        return template.format(
            org_id=quote(self.settings.coursera_org_id, safe=""),
            content_id=quote(content_id, safe=""),
        )

    def sensitive_values(self) -> tuple[str, ...]:
        return self.settings.coursera_secrets() + (
            (self._token,) if self._token else ()
        )

    async def _authorized_get(
        self, path: str, params: Mapping[str, Any], token: str | None
    ) -> httpx.Response:
        if not token:
            raise CourseraResponseContractError(
                "Coursera authentication returned an empty token"
            )
        return await self.http.request(
            "GET",
            f"{self.settings.coursera_base_url.rstrip('/')}/{path.lstrip('/')}",
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
    if isinstance(exc, (CourseraResponseContractError, ValueError)):
        return False
    return True
