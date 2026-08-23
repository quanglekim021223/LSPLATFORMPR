from __future__ import annotations

import base64
import logging
from collections.abc import Callable

import httpx
import pytest

from app.vendors.coursera.client import CourseraClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_coursera_authentication_contract_and_secrets_not_logged(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        expected = base64.b64encode(
            b"test-coursera-user:test-coursera-password"
        ).decode()
        assert request.headers["Authorization"] == f"Basic {expected}"
        assert request.headers["Accept"] == "application/json"
        assert request.content == b"grant_type=client_credentials"
        return response(request, 200, {"access_token": "run-token"})

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = CourseraClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        assert await client.authenticate() == "run-token"

    assert "test-coursera-password" not in caplog.text
    assert "run-token" not in caplog.text


@pytest.mark.asyncio
async def test_401_refreshes_once_and_retries_get_once(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls = {"auth": 0, "get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            calls["auth"] += 1
            return response(request, 200, {"access_token": f"token-{calls['auth']}"})
        calls["get"] += 1
        expected = "token-1" if calls["get"] == 1 else "token-2"
        assert request.headers["Authorization"] == f"Bearer {expected}"
        status_code = 401 if calls["get"] == 1 else 200
        return response(request, status_code, {"elements": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = CourseraClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        payload, _ = await client.get_json("/test-org/contents", {})

    assert payload == {"elements": []}
    assert calls == {"auth": 2, "get": 2}


@pytest.mark.asyncio
async def test_second_401_is_not_retried(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls = {"auth": 0, "get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            calls["auth"] += 1
            return response(request, 200, {"access_token": f"token-{calls['auth']}"})
        calls["get"] += 1
        return response(request, 401, {})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = CourseraClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_json("/test-org/contents", {})

    assert calls == {"auth": 2, "get": 2}


@pytest.mark.asyncio
async def test_authentication_reuses_shared_http_retry(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(http_max_retries=1)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return response(request, 500, {"error": "temporary"})
        return response(request, 200, {"access_token": "token"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = CourseraClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        assert await client.authenticate() == "token"
    assert calls == 2


def test_detail_template_only_accepts_supported_fields(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        coursera_content_detail_path_template="/{org_id}/items/{content_id}"
    )
    client = CourseraClient(settings, httpx.AsyncClient())  # type: ignore[arg-type]
    assert client.content_detail_path("course/1") == "/test-org/items/course%2F1"

    invalid = settings_factory(
        coursera_content_detail_path_template="/{org_id}/{unknown}/{content_id}"
    )
    invalid_client = CourseraClient(invalid, httpx.AsyncClient())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="only supports"):
        invalid_client.content_detail_path("course-1")
