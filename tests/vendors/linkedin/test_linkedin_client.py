from __future__ import annotations

import logging
from collections.abc import Callable
from urllib.parse import parse_qs

import httpx
import pytest

from app.mocks.linkedin import token_payload
from app.vendors.linkedin.client import LinkedInClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_authentication_contract_and_secrets_not_logged(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert parse_qs(request.content.decode()) == {
            "grant_type": ["client_credentials"],
            "client_id": ["test-linkedin-client"],
            "client_secret": ["test-linkedin-secret"],
        }
        return response(request, 200, token_payload("run-token"))

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LinkedInClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        assert await client.authenticate() == "run-token"

    assert "test-linkedin-client" not in caplog.text
    assert "test-linkedin-secret" not in caplog.text
    assert "run-token" not in caplog.text


@pytest.mark.asyncio
async def test_401_refreshes_token_and_retries_exactly_once(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls = {"auth": 0, "get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/v2/accessToken":
            calls["auth"] += 1
            return response(
                request, 200, token_payload(f"token-{calls['auth']}")
            )
        calls["get"] += 1
        expected = "token-1" if calls["get"] == 1 else "token-2"
        assert request.headers["Authorization"] == f"Bearer {expected}"
        status_code = 401 if calls["get"] == 1 else 200
        return response(
            request,
            status_code,
            {"elements": [], "paging": {"links": []}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LinkedInClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        payload, _ = await client.get_json("/learningAssets", {"q": "criteria"})

    assert payload["elements"] == []
    assert calls == {"auth": 2, "get": 2}


@pytest.mark.asyncio
async def test_second_401_is_not_retried(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls = {"auth": 0, "get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/v2/accessToken":
            calls["auth"] += 1
            return response(
                request, 200, token_payload(f"token-{calls['auth']}")
            )
        calls["get"] += 1
        return response(request, 401, {})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LinkedInClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_json("/learningAssets", {"q": "criteria"})

    assert calls == {"auth": 2, "get": 2}


@pytest.mark.asyncio
async def test_authentication_reuses_http_retry(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(http_max_retries=1)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return response(request, 500, {"error": "temporary"})
        return response(request, 200, token_payload("token"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LinkedInClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        assert await client.authenticate() == "token"
    assert calls == 2


def test_asset_detail_query_comes_only_from_template(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        linkedin_asset_detail_query_template="q=criteria&customUrn={urn}"
    )
    client = LinkedInClient(settings, httpx.AsyncClient())  # type: ignore[arg-type]
    assert client.asset_detail_params("urn:li:course:1") == {
        "q": "criteria",
        "customUrn": "urn:li:course:1",
    }

    invalid = settings_factory(linkedin_asset_detail_query_template="q=criteria")
    invalid_client = LinkedInClient(invalid, httpx.AsyncClient())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must contain"):
        invalid_client.asset_detail_params("urn:li:course:1")
