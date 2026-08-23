from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.handlers.linkedin_handler import run_linkedin_ingestion
from app.models import RunStatus
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_full_pipeline_windows_pagination_raw_and_concurrency(
    settings_factory: Callable[..., object],
) -> None:
    history_start = datetime.now(UTC) - timedelta(days=15)
    settings = settings_factory(
        linkedin_history_start_time=history_start.isoformat(),
        linkedin_max_concurrency=2,
    )
    calls: Counter[str] = Counter()
    catalog_starts: list[int] = []
    history_calls: list[tuple[int, int, int]] = []
    active_details = 0
    maximum_details = 0
    catalog_raw = (
        b'{\n  "elements": [{"urn":"urn:li:course:1"},'
        b'{"urn":"urn:li:course:2"}], "paging": {"links": ['
        b'{"rel":"next","href":"/v2/learningAssets?start=2&count=2"}]}}'
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_details, maximum_details
        path = request.url.path
        calls[path] += 1
        if path == "/oauth/v2/accessToken":
            return response(request, 200, {"access_token": "shared-token"})
        assert request.headers["Authorization"] == "Bearer shared-token"
        if path == "/v2/learningAssets":
            detail_urn = request.url.params.get("assetFilteringCriteria.urn")
            if detail_urn is not None:
                active_details += 1
                maximum_details = max(maximum_details, active_details)
                await asyncio.sleep(0.01)
                active_details -= 1
                return response(
                    request,
                    200,
                    {"elements": [{"urn": detail_urn}], "paging": {"links": []}},
                )
            start = int(request.url.params["start"])
            catalog_starts.append(start)
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
                    "elements": [{"urn": "urn:li:course:3"}],
                    "paging": {"links": []},
                },
            )
        if path == "/v2/learningActivityReports":
            assert request.url.params["q"] == "criteria"
            assert request.url.params["timeOffset.unit"] == "DAY"
            duration = int(request.url.params["timeOffset.duration"])
            assert 1 <= duration <= 14
            started_at = int(request.url.params["startedAt"])
            start = int(request.url.params["start"])
            history_calls.append((started_at, duration, start))
            links = (
                [
                    {
                        "rel": "next",
                        "href": "/v2/learningActivityReports?start=2&count=2",
                    }
                ]
                if start == 0
                else []
            )
            elements = [{"id": 1}, {"id": 2}] if start == 0 else [{"id": 3}]
            return response(
                request, 200, {"elements": elements, "paging": {"links": links}}
            )
        raise AssertionError(request.url)

    summary = await run_linkedin_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog": 3,
        "course_detail": 3,
        "learning_history": 6,
    }
    assert summary.courses_succeeded == 3
    assert calls["/oauth/v2/accessToken"] == 1
    assert catalog_starts == [0, 2]
    assert len({started_at for started_at, _, _ in history_calls}) == 2
    assert [start for _, _, start in history_calls] == [0, 2, 0, 2]
    assert maximum_details == 2
    stored = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000000.json")  # type: ignore[attr-defined]
        if "course_catalog" in str(path)
    )
    assert stored.read_bytes() == catalog_raw
    assert json.loads(stored.read_text())["elements"][0]["urn"] == "urn:li:course:1"


@pytest.mark.asyncio
async def test_non_list_elements_warns_counts_zero_and_preserves_raw(
    settings_factory: Callable[..., object], caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_factory(
        linkedin_history_start_time=(datetime.now(UTC) - timedelta(hours=1)).isoformat()
    )
    invalid_raw = b'{\n  "elements": {"unexpected": true}, "paging": {"links": []}\n}'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/v2/accessToken":
            return response(request, 200, {"access_token": "token"})
        return httpx.Response(
            200,
            content=invalid_raw,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    caplog.set_level(logging.WARNING)
    summary = await run_linkedin_ingestion(
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
    raw_pages = list(settings.bronze_local_path.rglob("*.json"))  # type: ignore[attr-defined]
    response_pages = [path for path in raw_pages if path.name.startswith("offset=")]
    assert len(response_pages) == 2
    assert all(page.read_bytes() == invalid_raw for page in response_pages)


@pytest.mark.asyncio
async def test_missing_configuration_fails_linkedin_job_clearly(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(linkedin_asset_detail_query_template="")
    summary = await run_linkedin_ingestion(settings)  # type: ignore[arg-type]
    assert summary.status == RunStatus.FAILED
    assert summary.vendor == "linkedin"
    assert summary.error_message == (
        "Missing LinkedIn configuration: LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE"
    )
