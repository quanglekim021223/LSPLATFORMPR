from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from app.clients.skillup_client import SkillUpClient
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_skillup_client_sends_api_key_and_reuses_retry(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(http_max_retries=1)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        assert request.headers["x-api-key"] == "test-skillup-key"
        if attempts == 1:
            return response(request, 500, {"error": "temporary"})
        return response(request, 200, {"items": [], "hasNextPage": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = SkillUpClient(settings, http_client, sleep=no_sleep)  # type: ignore[arg-type]
        payload, _ = await client.get_json(
            "https://skillup-intelligence.test", "/taxonomy", {}
        )

    assert payload == {"items": [], "hasNextPage": False}
    assert attempts == 2
