from __future__ import annotations

import json
import logging
from collections.abc import Callable

import httpx
import pytest

from app.clients.levelup_client import LevelUpClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_authentication_contract_and_secrets_not_logged(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory()
    seen_body: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_body.update(json.loads(request.content))
        assert request.headers["X-API-Key"] == "test-private-key"
        assert request.headers["x-api-version"] == "2"
        return response(request, 200, "run-token")

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LevelUpClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        assert await client.authenticate() == "run-token"

    assert seen_body == {
        "username": "test-user",
        "password": "test-password",
        "privateKey": "test-private-key",
    }
    assert "test-password" not in caplog.text
    assert "test-private-key" not in caplog.text
    assert "run-token" not in caplog.text


@pytest.mark.asyncio
async def test_401_refreshes_token_once(settings_factory: Callable[..., object]) -> None:
    settings = settings_factory()
    auth_calls = 0
    get_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal auth_calls, get_calls
        if request.url.path == "/authenticate":
            auth_calls += 1
            return response(request, 200, f"token-{auth_calls}")
        get_calls += 1
        if get_calls == 1:
            assert request.headers["Authorization"] == "token-1"
            return response(request, 401, {})
        assert request.headers["Authorization"] == "token-2"
        return response(request, 200, {"courses": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LevelUpClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        payload, _ = await client.get_json("/courses", {})
    assert payload == {"courses": []}
    assert auth_calls == 2
    assert get_calls == 2


@pytest.mark.asyncio
async def test_401_is_not_refreshed_more_than_once(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()
    calls = {"auth": 0, "get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            calls["auth"] += 1
            return response(request, 200, f"token-{calls['auth']}")
        calls["get"] += 1
        return response(request, 401, {})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = LevelUpClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_json("/courses", {})
    assert calls == {"auth": 2, "get": 2}
