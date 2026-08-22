from __future__ import annotations

import httpx
import pytest

from app.helpers.http_client import RetryingHttpClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_retryable_statuses_honor_retry_after(status_code: int) -> None:
    attempts = 0
    waits: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                status_code, headers={"Retry-After": "0"}, request=request
            )
        return response(request, 200, {"ok": True})

    async def capture_sleep(seconds: float) -> None:
        waits.append(seconds)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response_value = await RetryingHttpClient(
            client, 3, sleep=capture_sleep, jitter=lambda: 0
        ).request("GET", "https://example.test/resource")

    assert response_value.status_code == 200
    assert attempts == 2
    assert waits == [0]


@pytest.mark.asyncio
async def test_timeout_is_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return response(request, 200, {})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RetryingHttpClient(
            client, 1, sleep=no_sleep, jitter=lambda: 0
        ).request("GET", "https://example.test/resource")
    assert result.status_code == 200
    assert attempts == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 403, 404])
async def test_non_retryable_4xx_is_not_retried(status_code: int) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return response(request, status_code, {})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RetryingHttpClient(client, 3, sleep=no_sleep).request(
            "GET", "https://example.test/resource"
        )
    assert result.status_code == status_code
    assert attempts == 1
