from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.clients.datacamp_client import DataCampClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_client_sends_bearer_accept_and_uses_shared_retry(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(http_max_retries=1)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        assert request.headers["Authorization"] == "Bearer test-datacamp-token"
        assert request.headers["Accept"] == "application/json"
        if attempts == 1:
            return response(request, 500, {"error": "temporary"})
        return response(request, 200, {"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = DataCampClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        payload, _ = await client.get_json("/v1/catalog/live-courses")

    assert payload == {"ok": True}
    assert attempts == 2
