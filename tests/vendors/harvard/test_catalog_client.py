from __future__ import annotations

import base64
from collections.abc import Callable
from urllib.parse import parse_qs

import httpx
import pytest

from app.config import Settings
from app.vendors.harvard.catalog_client import HarvardCatalogClient
from app.vendors.harvard.models import vendor_config
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_basic_auth_form_and_refreshes_401_exactly_once(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    token_calls = 0
    get_tokens: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        if request.url.path == "/v1/api/oauth/v2/accesstoken":
            token_calls += 1
            credentials = base64.b64decode(
                request.headers["Authorization"].removeprefix("Basic ")
            ).decode()
            assert credentials == "test-hmm-client:test-hmm-secret"
            assert request.headers["Content-Type"].startswith(
                "application/x-www-form-urlencoded"
            )
            form = parse_qs(request.content.decode())
            assert form == {
                "grant_type": ["client_credentials"],
                "scope": ["hbp.org.api/catalog.read"],
            }
            return response(request, 200, {"access_token": f"token-{token_calls}"})

        get_tokens.append(request.headers["Authorization"])
        if len(get_tokens) == 1:
            return response(request, 401, {"message": "expired"})
        return response(request, 200, {"count": 0, "list": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = HarvardCatalogClient(
            settings,
            vendor_config(settings, "harvard_hmm"),
            http,
            sleep=no_sleep,
        )
        payload, _ = await client.get_json("/api/catalog/test-hmm-org", {})

    assert payload["list"] == []
    assert token_calls == 2
    assert get_tokens == ["Bearer token-1", "Bearer token-2"]


@pytest.mark.asyncio
async def test_second_401_is_returned_without_another_refresh(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    token_calls = 0
    get_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls, get_calls
        if request.method == "POST":
            token_calls += 1
            return response(request, 200, {"access_token": f"token-{token_calls}"})
        get_calls += 1
        return response(request, 401, {"message": "still unauthorized"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = HarvardCatalogClient(
            settings,
            vendor_config(settings, "harvard_hmm"),
            http,
            sleep=no_sleep,
        )
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_json("/api/catalog/test-hmm-org", {})

    assert token_calls == 2
    assert get_calls == 2


@pytest.mark.asyncio
async def test_authentication_uses_shared_http_retry(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(http_max_retries=1)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return response(request, 500, {"message": "temporary"})
        return response(request, 200, {"access_token": "token"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = HarvardCatalogClient(
            settings,
            vendor_config(settings, "harvard_hmm"),
            http,
            sleep=no_sleep,
            jitter=lambda: 0.0,
        )
        assert await client.authenticate() == "token"

    assert calls == 2
