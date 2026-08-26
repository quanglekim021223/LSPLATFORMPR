from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.mocks.coursera import course_payload, enrollment_payload, token_payload
from app.models import RunStatus
from app.repositories import CheckpointStore
from app.services.coursera.course_catalog import CATALOG_DOMAIN
from app.services.coursera.learning_history import (
    DAILY_SYNC_SCOPE,
    FULL_SYNC_SCOPE,
    WEEKLY_SYNC_SCOPE,
)
from app.services.coursera.service import _monthly_sync_due, run_coursera_ingestion
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_full_pipeline_pagination_raw_and_detail_concurrency(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(coursera_max_concurrency=2)
    calls: Counter[str] = Counter()
    starts: dict[str, list[int]] = {"catalog": [], "history": []}
    active_details = 0
    maximum_details = 0
    catalog_page = {
        "elements": [
            course_payload("c1", "Course One"),
            course_payload("c2", "Course Two"),
        ],
        "paging": {"next": "2", "total": 3},
        "linked": {},
    }
    catalog_raw = json.dumps(catalog_page, indent=2).encode() + b"\n"

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_details, maximum_details
        path = request.url.path
        calls[path] += 1
        if path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("shared-token"))
        assert request.headers["Authorization"] == "Bearer shared-token"
        if path == "/test-org/contents":
            assert "modifiedSinceTimestamp" not in request.url.params
            start = int(request.url.params["start"])
            starts["catalog"].append(start)
            if start == 0:
                return httpx.Response(
                    200,
                    content=catalog_raw,
                    headers={"Content-Type": "application/json"},
                    request=request,
                )
            return response(
                request,
                200,
                {
                    "elements": [course_payload("c3", "Course Three")],
                    "paging": {"total": 3},
                    "linked": {},
                },
            )
        if path == "/test-org/enrollmentReports":
            assert request.url.params["includeS12n"] == "true"
            assert request.url.params["includeDeletedMembers"] == "true"
            assert request.url.params["includeExpiredContracts"] == "true"
            assert "lastActivityAfter" not in request.url.params
            start = int(request.url.params["start"])
            starts["history"].append(start)
            elements = (
                [
                    enrollment_payload("e1", "c1", completed=True),
                    enrollment_payload("e2", "c2", completed=False),
                ]
                if start == 0
                else [enrollment_payload("e3", "c3", completed=True)]
            )
            paging = {"next": "2", "total": 3} if start == 0 else {"total": 3}
            return response(
                request,
                200,
                {"elements": elements, "paging": paging, "linked": {}},
            )
        if path.startswith("/test-org/contents/") and path.endswith("/detail"):
            active_details += 1
            maximum_details = max(maximum_details, active_details)
            await asyncio.sleep(0.01)
            active_details -= 1
            content_id = path.removeprefix("/test-org/contents/").removesuffix(
                "/detail"
            )
            return response(
                request,
                200,
                {
                    "elements": [course_payload(content_id, "Course Detail")],
                    "paging": {},
                    "linked": {},
                },
            )
        raise AssertionError(request.url)

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.vendor == "coursera"
    assert summary.records_by_domain == {
        "course_catalog": 3,
        "course_detail": 3,
        "learning_history": 3,
    }
    assert summary.courses_succeeded == 3
    assert calls["/oauth2/client_credentials/token"] == 1
    assert starts == {"catalog": [0, 2], "history": [0, 2]}
    assert maximum_details == 2
    stored = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000000.json")  # type: ignore[attr-defined]
        if "course_catalog" in str(path)
    )
    assert stored.read_bytes() == catalog_raw
    assert json.loads(stored.read_text())["elements"][0]["contentId"] == "c1"


@pytest.mark.asyncio
async def test_non_list_elements_fail_without_entering_bronze(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    invalid_raw = b'{\n  "elements": {"unexpected": true}, "paging": {}\n}'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        return httpx.Response(
            200,
            content=invalid_raw,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert summary.records_by_domain == {}
    assert not list(settings.bronze_local_path.rglob("offset=*.json"))  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_one_detail_failure_is_isolated(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        if path == "/test-org/contents":
            return response(
                request,
                200,
                {
                    "elements": [
                        course_payload("bad", "Bad Course"),
                        course_payload("good", "Good Course"),
                    ],
                    "paging": {"total": 2},
                    "linked": {},
                },
            )
        if path == "/test-org/enrollmentReports":
            return response(
                request,
                200,
                {"elements": [], "paging": {"total": 0}, "linked": {}},
            )
        if "/bad/" in path:
            return response(request, 404, {"error": "missing"})
        return response(
            request,
            200,
            {
                "elements": [course_payload("good", "Good Course")],
                "paging": {},
                "linked": {},
            },
        )

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.courses_succeeded == 1
    assert summary.courses_failed == 1
    assert summary.records_by_domain["course_detail"] == 1
    assert await store.get_watermark("coursera", CATALOG_DOMAIN) is None


@pytest.mark.asyncio
async def test_second_run_uses_catalog_and_daily_history_watermarks(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    catalog_requests: list[dict[str, str]] = []
    history_requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        if request.url.path == "/test-org/contents":
            catalog_requests.append(dict(request.url.params))
            return response(
                request,
                200,
                {"elements": [], "paging": {"total": 0}, "linked": {}},
            )
        if request.url.path == "/test-org/enrollmentReports":
            history_requests.append(dict(request.url.params))
            return response(
                request,
                200,
                {"elements": [], "paging": {"total": 0}, "linked": {}},
            )
        raise AssertionError(request.url)

    first = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert first.status == RunStatus.SUCCEEDED
    catalog_watermark = await store.get_watermark("coursera", CATALOG_DOMAIN)
    history_watermark = await store.get_watermark(
        "coursera", "learning_history", DAILY_SYNC_SCOPE
    )
    assert catalog_watermark is not None
    assert history_watermark is not None
    assert len(catalog_watermark) == 10
    assert len(history_watermark) == 13
    assert "modifiedSinceTimestamp" not in catalog_requests[0]
    assert "lastActivityAfter" not in history_requests[0]
    assert await store.get_watermark(
        "coursera", "learning_history", WEEKLY_SYNC_SCOPE
    ) == history_watermark
    assert await store.get_watermark(
        "coursera", "learning_history", FULL_SYNC_SCOPE
    ) == history_watermark

    catalog_requests.clear()
    history_requests.clear()
    second = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert second.status == RunStatus.SUCCEEDED
    assert catalog_requests[0]["modifiedSinceTimestamp"] == catalog_watermark
    assert history_requests[0]["lastActivityAfter"] == history_watermark


@pytest.mark.asyncio
async def test_weekly_history_sync_reads_configured_lookback(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(coursera_history_lookback_days=90)
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    now = datetime.now(UTC)
    recent_milliseconds = str(int(now.timestamp() * 1000))
    await store.set_watermark(
        "coursera", "learning_history", recent_milliseconds, "run", FULL_SYNC_SCOPE
    )
    await store.set_watermark(
        "coursera",
        "learning_history",
        str(int((now - timedelta(days=8)).timestamp() * 1000)),
        "run",
        WEEKLY_SYNC_SCOPE,
    )
    history_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        if request.url.path == "/test-org/enrollmentReports":
            history_params.update(request.url.params)
        return response(
            request,
            200,
            {"elements": [], "paging": {"total": 0}, "linked": {}},
        )

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    new_daily = await store.get_watermark(
        "coursera", "learning_history", DAILY_SYNC_SCOPE
    )
    assert new_daily is not None
    assert int(new_daily) - int(history_params["lastActivityAfter"]) == (
        90 * 24 * 60 * 60 * 1000
    )
    assert await store.get_watermark(
        "coursera", "learning_history", WEEKLY_SYNC_SCOPE
    ) == new_daily


def test_monthly_history_sync_uses_ingestion_timezone_calendar_month() -> None:
    last_sync = str(
        int(datetime(2025, 12, 31, 17, 0, tzinfo=UTC).timestamp() * 1000)
    )

    assert not _monthly_sync_due(
        last_sync,
        datetime(2026, 1, 31, 16, 59, 59, tzinfo=UTC),
        "Asia/Ho_Chi_Minh",
    )
    assert _monthly_sync_due(
        last_sync,
        datetime(2026, 1, 31, 17, 0, tzinfo=UTC),
        "Asia/Ho_Chi_Minh",
    )


@pytest.mark.asyncio
async def test_removed_catalog_content_is_stored_without_detail_request(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    detail_calls = 0
    removed = {
        **course_payload("removed", "Removed Course"),
        "changes": [{"changeType": "REMOVED", "programIds": ["program"]}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal detail_calls
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        if request.url.path == "/test-org/contents":
            return response(
                request,
                200,
                {"elements": [removed], "paging": {"total": 1}, "linked": {}},
            )
        if request.url.path == "/test-org/enrollmentReports":
            return response(
                request,
                200,
                {"elements": [], "paging": {"total": 0}, "linked": {}},
            )
        if "/contents/removed/" in request.url.path:
            detail_calls += 1
        raise AssertionError(request.url)

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain["course_catalog"] == 1
    assert detail_calls == 0
    assert await store.get_watermark("coursera", CATALOG_DOMAIN) is not None


@pytest.mark.asyncio
async def test_failed_history_does_not_advance_daily_watermark(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    now = datetime.now(UTC)
    recent_milliseconds = str(int(now.timestamp() * 1000))
    old_daily = str(int((now - timedelta(days=1)).timestamp() * 1000))
    await store.set_watermark(
        "coursera", "learning_history", recent_milliseconds, "run", FULL_SYNC_SCOPE
    )
    await store.set_watermark(
        "coursera", "learning_history", recent_milliseconds, "run", WEEKLY_SYNC_SCOPE
    )
    await store.set_watermark(
        "coursera", "learning_history", old_daily, "run", DAILY_SYNC_SCOPE
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, token_payload("token"))
        if request.url.path == "/test-org/enrollmentReports":
            return response(request, 500, {"error": "unavailable"})
        return response(
            request,
            200,
            {"elements": [], "paging": {"total": 0}, "linked": {}},
        )

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert await store.get_watermark(
        "coursera", "learning_history", DAILY_SYNC_SCOPE
    ) == old_daily


@pytest.mark.asyncio
async def test_missing_configuration_fails_only_coursera_job_clearly(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(coursera_content_detail_path_template="")
    summary = await run_coursera_ingestion(settings)  # type: ignore[arg-type]
    assert summary.status == RunStatus.FAILED
    assert summary.vendor == "coursera"
    assert summary.error_message == (
        "Missing Coursera configuration: COURSERA_CONTENT_DETAIL_PATH_TEMPLATE"
    )
