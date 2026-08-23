from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable

import httpx
import pytest

from app.handlers.coursera_handler import run_coursera_ingestion
from app.models import RunStatus
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
    catalog_raw = (
        b'{\n  "elements": [{"contentId":"c1"},{"contentId":"c2"}],'
        b' "paging": {"next": 2}\n}'
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_details, maximum_details
        path = request.url.path
        calls[path] += 1
        if path == "/oauth2/client_credentials/token":
            return response(request, 200, {"access_token": "shared-token"})
        assert request.headers["Authorization"] == "Bearer shared-token"
        if path == "/test-org/contents":
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
                {"elements": [{"contentId": "c3"}], "paging": {}},
            )
        if path == "/test-org/enrollmentReports":
            assert request.url.params["includeS12n"] == "true"
            start = int(request.url.params["start"])
            starts["history"].append(start)
            elements = [{"id": "e1"}, {"id": "e2"}] if start == 0 else [{"id": "e3"}]
            paging = {"next": 2} if start == 0 else {}
            return response(request, 200, {"elements": elements, "paging": paging})
        if path.startswith("/test-org/contents/") and path.endswith("/detail"):
            active_details += 1
            maximum_details = max(maximum_details, active_details)
            await asyncio.sleep(0.01)
            active_details -= 1
            return response(request, 200, {"elements": [{"detail": path}]})
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
async def test_non_list_elements_warns_counts_zero_and_preserves_raw(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory()
    invalid_raw = b'{\n  "elements": {"unexpected": true}, "paging": {}\n}'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/client_credentials/token":
            return response(request, 200, {"access_token": "token"})
        return httpx.Response(
            200,
            content=invalid_raw,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    caplog.set_level(logging.WARNING)
    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog": 0,
        "learning_history": 0,
    }
    assert caplog.text.count("elements is not a list") == 2
    raw_pages = list(settings.bronze_local_path.rglob("offset=000000.json"))  # type: ignore[attr-defined]
    assert len(raw_pages) == 2
    assert all(page.read_bytes() == invalid_raw for page in raw_pages)


@pytest.mark.asyncio
async def test_one_detail_failure_is_isolated(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/oauth2/client_credentials/token":
            return response(request, 200, {"access_token": "token"})
        if path == "/test-org/contents":
            return response(
                request,
                200,
                {
                    "elements": [{"contentId": "bad"}, {"contentId": "good"}],
                    "paging": {},
                },
            )
        if path == "/test-org/enrollmentReports":
            return response(request, 200, {"elements": [], "paging": {}})
        if "/bad/" in path:
            return response(request, 404, {"error": "missing"})
        return response(request, 200, {"elements": [{"id": "good"}]})

    summary = await run_coursera_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.courses_succeeded == 1
    assert summary.courses_failed == 1
    assert summary.records_by_domain["course_detail"] == 1


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
