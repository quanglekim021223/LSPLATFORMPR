from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from app.mocks.app import app as mock_app
from app.mocks.settings import get_mock_settings


@pytest.fixture(autouse=True)
def clear_mock_settings_cache() -> Iterator[None]:
    get_mock_settings.cache_clear()
    yield
    get_mock_settings.cache_clear()


@pytest.mark.asyncio
async def test_direct_api_key_and_bearer_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOCK_SKILLUP_API_KEY", "vendor-side-api-key")
    monkeypatch.setenv("MOCK_DATACAMP_TOKEN", "vendor-side-bearer")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app),
        base_url="http://mock",
    ) as client:
        old_key = await client.get(
            "/skillup/taxonomy",
            headers={"x-api-key": "mock-skillup-key"},
        )
        configured_key = await client.get(
            "/skillup/taxonomy",
            headers={"x-api-key": "vendor-side-api-key"},
        )
        old_token = await client.get(
            "/datacamp/v1/catalog/live-courses",
            headers={
                "Authorization": "Bearer mock-datacamp-token",
                "Accept": "application/json",
            },
        )
        configured_token = await client.get(
            "/datacamp/v1/catalog/live-courses",
            headers={
                "Authorization": "Bearer vendor-side-bearer",
                "Accept": "application/json",
            },
        )

    assert old_key.status_code == 401
    assert configured_key.status_code == 200
    assert old_token.status_code == 401
    assert configured_token.status_code == 200


@pytest.mark.asyncio
async def test_client_credentials_issue_configured_access_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOCK_LINKEDIN_CLIENT_ID", "vendor-client-id")
    monkeypatch.setenv("MOCK_LINKEDIN_CLIENT_SECRET", "vendor-client-secret")
    monkeypatch.setenv("MOCK_LINKEDIN_ACCESS_TOKEN", "vendor-access-token")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app),
        base_url="http://mock",
    ) as client:
        denied = await client.post(
            "/linkedin/oauth/v2/accessToken",
            data={
                "grant_type": "client_credentials",
                "client_id": "wrong",
                "client_secret": "wrong",
            },
        )
        token = await client.post(
            "/linkedin/oauth/v2/accessToken",
            data={
                "grant_type": "client_credentials",
                "client_id": "vendor-client-id",
                "client_secret": "vendor-client-secret",
            },
        )

    assert denied.status_code == 401
    assert token.status_code == 200
    assert token.json()["access_token"] == "vendor-access-token"
