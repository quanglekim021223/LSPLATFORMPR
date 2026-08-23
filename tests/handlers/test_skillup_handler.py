from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable

import httpx
import pytest

from app.handlers.skillup_handler import run_skillup_ingestion
from app.models import RunStatus
from tests.conftest import no_sleep, response


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
    taxonomy_raw = (
        b'{\n  "items": [{"id": 1}, {"id": 2}], '
        b'"pageNumber": 1, "hasNextPage": true\n}\n'
    )

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
                {"items": [{"id": 3}], "pageNumber": 2, "hasNextPage": False},
            )

        if path == "/employees/skills-profile":
            page = int(request.url.params["pageNumber"])
            pages[path].append(page)
            items = [{"employeeId": 1}, {"employeeId": 2}] if page == 1 else [{"employeeId": 3}]
            return response(
                request,
                200,
                {
                    "items": items,
                    "metadata": {"pageNumber": page, "totalPages": 2},
                },
            )

        if path == "/v3/reports":
            page = int(request.url.params["PageNo"])
            pages[path].append(page)
            reports = [{"id": 1}, {"id": 2}] if page == 1 else [{"id": 3}]
            return response(
                request,
                200,
                {
                    "reports": reports,
                    "pageNumber": page,
                    "totalPages": 2,
                    "hasNextPage": page < 2,
                },
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
    assert json.loads(taxonomy_page.read_text())["items"] == [{"id": 1}, {"id": 2}]


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
            return response(
                request,
                200,
                {"items": [{"employeeId": 1}], "hasNextPage": False},
            )
        if path == "/v3/reports":
            return response(
                request,
                200,
                {"reports": [{"id": 1}], "hasNextPage": False, "totalPages": 1},
            )
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
            return response(request, 200, {"items": [], "hasNextPage": False})
        if request.url.path == "/employees/skills-profile":
            return response(request, 200, {"items": [], "hasNextPage": False})
        if request.url.path == "/v3/reports":
            return response(
                request,
                200,
                {"reports": [], "hasNextPage": False, "totalPages": 1},
            )
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
