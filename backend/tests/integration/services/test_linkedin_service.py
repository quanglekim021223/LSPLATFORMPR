from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.mocks.linkedin import activity_report_payload, asset_payload, token_payload
from app.models import RunStatus
from app.services.linkedin.service import run_linkedin_ingestion
from tests.conftest import no_sleep, response


def _page(
    elements: list[dict[str, object]],
    *,
    start: int,
    count: int = 2,
    total: int | None = None,
    next_start: int | None = None,
    metadata: bool = False,
) -> dict[str, object]:
    links = []
    if next_start is not None:
        links.append(
            {
                "type": "application/json",
                "rel": "next",
                "href": f"/v2/resource?start={next_start}&count={count}",
            }
        )
    payload: dict[str, object] = {
        "elements": elements,
        "paging": {
            "start": start,
            "count": count,
            "links": links,
            "total": len(elements) if total is None else total,
        },
    }
    if metadata:
        payload["metadata"] = {}
    return payload


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
    first_catalog_page = _page(
        [
            asset_payload("urn:li:course:1", "Course 1"),
            asset_payload("urn:li:course:2", "Course 2"),
        ],
        start=0,
        total=3,
        next_start=2,
        metadata=True,
    )
    catalog_raw = json.dumps(first_catalog_page, indent=2).encode()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_details, maximum_details
        path = request.url.path
        calls[path] += 1
        if path == "/oauth/v2/accessToken":
            return response(request, 200, token_payload("shared-token"))
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
                    _page(
                        [asset_payload(detail_urn, f"Detail {detail_urn}")],
                        start=0,
                        total=1,
                        metadata=True,
                    ),
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
                _page(
                    [asset_payload("urn:li:course:3", "Course 3")],
                    start=2,
                    total=3,
                    metadata=True,
                ),
            )
        if path == "/v2/learningActivityReports":
            assert request.url.params["q"] == "criteria"
            assert request.url.params["timeOffset.unit"] == "DAY"
            duration = int(request.url.params["timeOffset.duration"])
            assert 1 <= duration <= 14
            started_at = int(request.url.params["startedAt"])
            start = int(request.url.params["start"])
            history_calls.append((started_at, duration, start))
            elements = (
                [
                    activity_report_payload(1, started_at),
                    activity_report_payload(2, started_at),
                ]
                if start == 0
                else [activity_report_payload(3, started_at)]
            )
            return response(
                request,
                200,
                _page(
                    elements,
                    start=start,
                    total=3,
                    next_start=2 if start == 0 else None,
                ),
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
async def test_invalid_contract_fails_without_writing_raw_to_bronze(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        linkedin_history_start_time=(datetime.now(UTC) - timedelta(hours=1)).isoformat()
    )
    invalid_raw = b'{\n  "elements": {"unexpected": true}, "paging": {"links": []}\n}'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/v2/accessToken":
            return response(request, 200, token_payload("token"))
        return httpx.Response(
            200,
            content=invalid_raw,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    summary = await run_linkedin_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert summary.records_by_domain == {}
    raw_pages = list(settings.bronze_local_path.rglob("*.json"))  # type: ignore[attr-defined]
    response_pages = [path for path in raw_pages if path.name.startswith("offset=")]
    assert response_pages == []


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
