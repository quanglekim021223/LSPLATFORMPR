from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable

import httpx
import pytest

from app.mocks.skillup import assessment_report, skill_profile, taxonomy_item
from app.models import RunStatus
from app.services.skillup.service import run_skillup_ingestion
from tests.conftest import no_sleep, response


def page_payload(
    key: str,
    records: list[dict[str, object]],
    *,
    page_number: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
) -> dict[str, object]:
    return {
        key: records,
        "pageNumber": page_number,
        "totalPages": total_pages,
        "totalCount": len(records) if total_count is None else total_count,
        "hasPreviousPage": page_number > 1,
        "hasNextPage": page_number < total_pages,
    }


def valid_response(path: str) -> dict[str, object]:
    if path == "/taxonomy":
        return page_payload("items", [taxonomy_item()])
    if path == "/employees/skills-profile":
        return page_payload("items", [skill_profile()])
    if path == "/v3/reports":
        return page_payload("reports", [assessment_report()])
    raise AssertionError(path)


@pytest.mark.asyncio
async def test_skillup_three_domains_paginate_and_preserve_raw(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()
    pages: dict[str, list[int]] = {
        "/taxonomy": [],
        "/employees/skills-profile": [],
        "/v3/reports": [],
    }
    first_taxonomy_page = page_payload(
        "items",
        [taxonomy_item(0), taxonomy_item(1)],
        total_pages=2,
        total_count=3,
    )
    taxonomy_raw = json.dumps(first_taxonomy_page, indent=2).encode() + b"\n"

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls[path] += 1
        assert request.headers["x-api-key"] == "test-skillup-key"
        assert "SkillProfileModifiedSince" not in request.url.params
        assert "searchText" not in request.url.params
        assert "includeSections" not in request.url.params
        if path == "/v3/reports":
            assert request.url.params["startDate"] == "2000-01-01T00:00:00Z"
            assert "endDate" in request.url.params
        else:
            assert "startDate" not in request.url.params
            assert "endDate" not in request.url.params

        if path == "/taxonomy":
            page = int(request.url.params["PageNumber"])
            pages[path].append(page)
            if page == 1:
                return httpx.Response(
                    200,
                    content=taxonomy_raw,
                    headers={"Content-Type": "application/json"},
                    request=request,
                )
            return response(
                request,
                200,
                page_payload(
                    "items",
                    [taxonomy_item(2)],
                    page_number=2,
                    total_pages=2,
                    total_count=3,
                ),
            )

        if path == "/employees/skills-profile":
            page = int(request.url.params["pageNumber"])
            pages[path].append(page)
            items = (
                [skill_profile(0), skill_profile(1)]
                if page == 1
                else [skill_profile(2)]
            )
            return response(
                request,
                200,
                page_payload(
                    "items",
                    items,
                    page_number=page,
                    total_pages=2,
                    total_count=3,
                ),
            )

        if path == "/v3/reports":
            page = int(request.url.params["PageNo"])
            pages[path].append(page)
            reports = (
                [assessment_report(0), assessment_report(1)]
                if page == 1
                else [assessment_report(2)]
            )
            return response(
                request,
                200,
                page_payload(
                    "reports",
                    reports,
                    page_number=page,
                    total_pages=2,
                    total_count=3,
                ),
            )
        raise AssertionError(request.url)

    summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.vendor == "skillup"
    assert summary.records_by_domain == {
        "assessment_history": 3,
        "skill_inventory": 3,
        "skill_taxonomy": 3,
    }
    assert calls == {
        "/taxonomy": 2,
        "/employees/skills-profile": 2,
        "/v3/reports": 2,
    }
    assert pages == {
        "/taxonomy": [1, 2],
        "/employees/skills-profile": [1, 2],
        "/v3/reports": [1, 2],
    }
    taxonomy_page = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
        if "skill_taxonomy" in str(path)
    )
    assert taxonomy_page.read_bytes() == taxonomy_raw
    assert json.loads(taxonomy_page.read_text())["items"] == [
        taxonomy_item(0),
        taxonomy_item(1),
    ]


@pytest.mark.asyncio
async def test_skillup_domain_failure_does_not_stop_other_domains(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    called: Counter[str] = Counter()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        called[path] += 1
        if path == "/taxonomy":
            return response(request, 500, {"error": "taxonomy unavailable"})
        if path == "/employees/skills-profile":
            return response(request, 200, valid_response(path))
        if path == "/v3/reports":
            return response(request, 200, valid_response(path))
        raise AssertionError(request.url)

    summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.records_by_domain == {
        "assessment_history": 1,
        "skill_inventory": 1,
    }
    assert called == {
        "/taxonomy": 1,
        "/employees/skills-profile": 1,
        "/v3/reports": 1,
    }
    stored_domains = {
        path.parents[2].name
        for path in settings.bronze_local_path.rglob("offset=*.json")  # type: ignore[attr-defined]
    }
    assert stored_domains == {"skill_inventory", "assessment_history"}

    next_summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert next_summary.run_id != summary.run_id
    assert called == {
        "/taxonomy": 2,
        "/employees/skills-profile": 2,
        "/v3/reports": 2,
    }


@pytest.mark.asyncio
async def test_skillup_optional_parameters_are_sent_only_when_provided(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    seen: dict[str, dict[str, str]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen[request.url.path] = dict(request.url.params)
        if request.url.path == "/taxonomy":
            return response(request, 200, page_payload("items", []))
        if request.url.path == "/employees/skills-profile":
            return response(request, 200, page_payload("items", []))
        if request.url.path == "/v3/reports":
            return response(request, 200, page_payload("reports", []))
        raise AssertionError(request.url)

    summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
        taxonomy_params={"SearchText": "python"},
        skill_profile_modified_since="2026-08-01T00:00:00Z",
        search_text="alice",
        include_sections=True,
        start_date="2026-08-01T00:00:00Z",
        end_date="2026-08-22T00:00:00Z",
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert seen["/taxonomy"]["SearchText"] == "python"
    assert seen["/employees/skills-profile"]["SkillProfileModifiedSince"] == (
        "2026-08-01T00:00:00Z"
    )
    assert seen["/employees/skills-profile"]["searchText"] == "alice"
    assert seen["/v3/reports"]["includeSections"] == "true"
    assert seen["/v3/reports"]["startDate"] == "2026-08-01T00:00:00Z"
    assert seen["/v3/reports"]["endDate"] == "2026-08-22T00:00:00Z"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("invalid_path", "invalid_domain"),
    [
        ("/taxonomy", "skill_taxonomy"),
        ("/employees/skills-profile", "skill_inventory"),
        ("/v3/reports", "assessment_history"),
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
            records_key = "reports" if invalid_path == "/v3/reports" else "items"
            records = payload[records_key]
            assert isinstance(records, list)
            record = records[0]
            assert isinstance(record, dict)
            required_field = {
                "/taxonomy": "displayName",
                "/employees/skills-profile": "externalEmployeeId",
                "/v3/reports": "candidateFullName",
            }[invalid_path]
            del record[required_field]
        return response(request, 200, payload)

    summary = await run_skillup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert not list(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            f"skillup/{invalid_domain}/**/offset=*.json"
        )
    )
