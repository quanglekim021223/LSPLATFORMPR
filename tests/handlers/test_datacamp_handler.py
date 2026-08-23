from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable

import httpx
import pytest

from app.handlers.datacamp_handler import run_datacamp_ingestion
from app.models import RunStatus
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_three_domains_event_pagination_and_raw_bronze(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()
    pages: list[int] = []
    live_raw = b'{\n  "data": [{"unchanged": true}, {"id": 2}]\n}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls[path] += 1
        assert request.headers["Authorization"] == "Bearer test-datacamp-token"
        assert request.headers["Accept"] == "application/json"
        if path == "/v1/catalog/live-courses":
            assert not request.url.params
            return httpx.Response(
                200,
                content=live_raw,
                headers={"Content-Type": "application/json"},
                request=request,
            )
        if path == "/v1/catalog/archived-courses":
            assert not request.url.params
            return response(request, 200, {"data": [{"id": "archived-1"}]})
        if path == "/v1/events":
            assert "contentType" not in request.url.params
            assert "eventType" not in request.url.params
            assert "from" not in request.url.params
            assert "to" not in request.url.params
            page = int(request.url.params["page"])
            pages.append(page)
            events = [{"id": 1}, {"id": 2}] if page == 1 else [{"id": 3}]
            return response(
                request,
                200,
                {"events": events, "meta": {"numberOfPages": 2}},
            )
        raise AssertionError(request.url)

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.vendor == "datacamp"
    assert summary.records_by_domain == {
        "course_catalog_archived": 1,
        "course_catalog_live": 2,
        "learning_history": 3,
    }
    assert calls == {
        "/v1/catalog/live-courses": 1,
        "/v1/catalog/archived-courses": 1,
        "/v1/events": 2,
    }
    assert pages == [1, 2]
    live_page = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
        if "course_catalog_live" in str(path)
    )
    assert live_page.read_bytes() == live_raw
    assert json.loads(live_page.read_text())["data"] == [
        {"unchanged": True},
        {"id": 2},
    ]


@pytest.mark.asyncio
async def test_domain_failure_is_isolated(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()

    def handler(request: httpx.Request) -> httpx.Response:
        calls[request.url.path] += 1
        if request.url.path == "/v1/catalog/live-courses":
            return response(request, 500, {"error": "unavailable"})
        if request.url.path == "/v1/catalog/archived-courses":
            return response(request, 200, {"anything": "raw"})
        if request.url.path == "/v1/events":
            return response(
                request,
                200,
                {"events": [{"id": 1}], "meta": {"numberOfPages": 1}},
            )
        raise AssertionError(request.url)

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.records_by_domain == {
        "course_catalog_archived": 0,
        "learning_history": 1,
    }
    assert calls == {
        "/v1/catalog/live-courses": 1,
        "/v1/catalog/archived-courses": 1,
        "/v1/events": 1,
    }


@pytest.mark.asyncio
async def test_catalog_non_list_data_warns_but_preserves_raw(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory()
    invalid_raw = b'{\n  "data": {"course": "still raw"}\n}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/live-courses":
            return httpx.Response(
                200,
                content=invalid_raw,
                headers={"Content-Type": "application/json"},
                request=request,
            )
        if request.url.path == "/v1/catalog/archived-courses":
            return response(request, 200, {"data": []})
        return response(
            request, 200, {"events": [], "meta": {"numberOfPages": 1}}
        )

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain["course_catalog_live"] == 0
    assert "live catalog response field 'data' is not a list" in caplog.text
    live_page = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
        if "course_catalog_live" in str(path)
    )
    assert live_page.read_bytes() == invalid_raw


@pytest.mark.asyncio
async def test_event_optional_parameters_are_sent(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    event_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            event_params.update(request.url.params)
            return response(
                request,
                200,
                {"events": [], "meta": {"numberOfPages": 1}},
            )
        return response(request, 200, {})

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
        content_type="course",
        event_type="completed",
        from_value="2026-01-01T00:00:00Z",
        to="2026-08-23T00:00:00Z",
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert event_params == {
        "page": "1",
        "pageSize": "2",
        "contentType": "course",
        "eventType": "completed",
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-08-23T00:00:00Z",
    }
