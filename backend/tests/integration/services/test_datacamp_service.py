from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from app.mocks.datacamp import course_payload, event_payload
from app.models import RunStatus
from app.repositories import CheckpointStore
from app.services.datacamp.learning_history import (
    DAILY_SYNC_SCOPE,
    FULL_SYNC_SCOPE,
    WEEKLY_SYNC_SCOPE,
)
from app.services.datacamp.service import _monthly_sync_due, run_datacamp_ingestion
from tests.conftest import no_sleep, response


def events_page(
    records: list[dict[str, object]],
    *,
    page: int = 1,
    page_size: int = 2,
    number_of_pages: int = 1,
) -> dict[str, object]:
    return {
        "data": records,
        "meta": {
            "page": page,
            "pageSize": page_size,
            "numberOfPages": number_of_pages,
        },
    }


def valid_response(path: str) -> dict[str, object]:
    if path == "/v1/catalog/live-courses":
        return {"data": [course_payload("course-live", "Live", live=True)]}
    if path == "/v1/catalog/archived-courses":
        return {
            "data": [
                course_payload(
                    "course-archived", "Archived", live=False, technology=None
                )
            ]
        }
    if path == "/v1/events":
        return events_page([event_payload(1)])
    raise AssertionError(path)


@pytest.mark.asyncio
async def test_three_domains_event_pagination_and_raw_bronze(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()
    pages: list[int] = []
    first_live_page = {
        "data": [
            course_payload("course-live-1", "Live One", live=True),
            course_payload("course-live-2", "Live Two", live=True),
        ]
    }
    live_raw = json.dumps(first_live_page, indent=2).encode() + b"\n"

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
            return response(request, 200, valid_response(path))
        if path == "/v1/events":
            assert "contentType" not in request.url.params
            assert "eventType" not in request.url.params
            assert request.url.params["from"] == "2000-01-01T00:00:00Z"
            assert "to" in request.url.params
            page = int(request.url.params["page"])
            pages.append(page)
            events = (
                [event_payload(1), event_payload(2)]
                if page == 1
                else [event_payload(3)]
            )
            return response(
                request,
                200,
                events_page(events, page=page, number_of_pages=2),
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
    assert json.loads(live_page.read_text()) == first_live_page


@pytest.mark.asyncio
async def test_events_backfill_then_use_daily_watermark(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    event_requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            event_requests.append(dict(request.url.params))
        return response(request, 200, valid_response(request.url.path))

    first = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert first.status == RunStatus.SUCCEEDED
    daily_watermark = await store.get_watermark(
        "datacamp", "learning_history", DAILY_SYNC_SCOPE
    )
    assert daily_watermark is not None
    assert event_requests[0]["from"] == "2000-01-01T00:00:00Z"
    assert event_requests[0]["to"] == daily_watermark
    assert await store.get_watermark(
        "datacamp", "learning_history", WEEKLY_SYNC_SCOPE
    ) == daily_watermark
    assert await store.get_watermark(
        "datacamp", "learning_history", FULL_SYNC_SCOPE
    ) == daily_watermark

    event_requests.clear()
    second = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert second.status == RunStatus.SUCCEEDED
    assert event_requests[0]["from"] == daily_watermark
    assert event_requests[0]["to"] != ""


@pytest.mark.asyncio
async def test_events_weekly_sync_reads_ninety_days(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(datacamp_events_lookback_days=90)
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    recent_sync = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    await store.set_watermark(
        "datacamp", "learning_history", recent_sync, "run", FULL_SYNC_SCOPE
    )
    await store.set_watermark(
        "datacamp", "learning_history", "2000-01-01T00:00:00Z", "run", WEEKLY_SYNC_SCOPE
    )
    event_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            event_params.update(request.url.params)
        return response(request, 200, valid_response(request.url.path))

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    start = datetime.fromisoformat(event_params["from"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(event_params["to"].replace("Z", "+00:00"))
    assert (end - start).days == 90


@pytest.mark.asyncio
async def test_events_monthly_sync_reads_full_configured_history(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        datacamp_events_start_time="2019-01-01T00:00:00Z"
    )
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    await store.set_watermark(
        "datacamp", "learning_history", "2000-01-01T00:00:00Z", "run", FULL_SYNC_SCOPE
    )
    event_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            event_params.update(request.url.params)
        return response(request, 200, valid_response(request.url.path))

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert event_params["from"] == "2019-01-01T00:00:00Z"
    assert datetime.fromisoformat(
        event_params["to"].replace("Z", "+00:00")
    ).tzinfo is UTC


def test_monthly_sync_uses_ingestion_timezone_calendar_month() -> None:
    last_sync = "2025-12-31T17:00:00Z"  # 2026-01-01 00:00 in Ho Chi Minh City

    assert not _monthly_sync_due(
        last_sync,
        datetime(2026, 1, 31, 16, 59, 59, tzinfo=UTC),
        "Asia/Ho_Chi_Minh",
    )
    assert _monthly_sync_due(
        last_sync,
        datetime(2026, 1, 31, 17, 0, 0, tzinfo=UTC),
        "Asia/Ho_Chi_Minh",
    )


@pytest.mark.asyncio
async def test_failed_events_do_not_advance_daily_watermark(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    old_daily = "2026-08-20T00:00:00Z"
    recent_sync = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    await store.set_watermark(
        "datacamp", "learning_history", old_daily, "run", DAILY_SYNC_SCOPE
    )
    await store.set_watermark(
        "datacamp", "learning_history", recent_sync, "run", WEEKLY_SYNC_SCOPE
    )
    await store.set_watermark(
        "datacamp", "learning_history", recent_sync, "run", FULL_SYNC_SCOPE
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            return response(request, 500, {"error": "unavailable"})
        return response(request, 200, valid_response(request.url.path))

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert await store.get_watermark(
        "datacamp", "learning_history", DAILY_SYNC_SCOPE
    ) == old_daily


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
            return response(request, 200, valid_response(request.url.path))
        if request.url.path == "/v1/events":
            return response(request, 200, valid_response(request.url.path))
        raise AssertionError(request.url)

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.records_by_domain == {
        "course_catalog_archived": 1,
        "learning_history": 1,
    }
    assert calls == {
        "/v1/catalog/live-courses": 1,
        "/v1/catalog/archived-courses": 1,
        "/v1/events": 1,
    }


@pytest.mark.asyncio
async def test_catalog_non_list_data_fails_without_entering_bronze(
    settings_factory: Callable[..., object],
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
        return response(request, 200, events_page([]))

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert "course_catalog_live" not in summary.records_by_domain
    assert not list(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            "datacamp/course_catalog_live/**/offset=*.json"
        )
    )


@pytest.mark.asyncio
async def test_event_optional_parameters_are_sent(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    event_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/events":
            event_params.update(request.url.params)
            return response(request, 200, events_page([]))
        return response(request, 200, {"data": []})

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("invalid_path", "invalid_domain"),
    [
        ("/v1/catalog/live-courses", "course_catalog_live"),
        ("/v1/catalog/archived-courses", "course_catalog_archived"),
        ("/v1/events", "learning_history"),
    ],
)
async def test_contract_invalid_response_does_not_enter_bronze(
    settings_factory: Callable[..., object],
    invalid_path: str,
    invalid_domain: str,
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        payload = valid_response(request.url.path)
        if request.url.path == invalid_path:
            records = payload["data"]
            assert isinstance(records, list)
            record = records[0]
            assert isinstance(record, dict)
            if invalid_path == "/v1/catalog/live-courses":
                record["live"] = False
            elif invalid_path == "/v1/catalog/archived-courses":
                record["live"] = True
            else:
                del record["user"]
        return response(request, 200, payload)

    summary = await run_datacamp_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert not list(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            f"datacamp/{invalid_domain}/**/offset=*.json"
        )
    )
