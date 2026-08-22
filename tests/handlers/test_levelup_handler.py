from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from app.handlers.levelup_handler import run_levelup_ingestion
from app.models import RunStatus
from app.repositories.checkpoint_repository import CheckpointStore
from tests.conftest import no_sleep, response


@pytest.mark.asyncio
async def test_catalog_pagination_filter_and_course_list_reuse(
    settings_factory: Callable[..., object], tmp_path: Path
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()
    first_raw_payload = (
        b'{\n  "totalItems": 3, "returnedItems": 2, "courses": ['
        b'{"id":"c1","vendor":null,"name":"Course 1"},'
        b'{"id":"linkedin","vendor":"LinkedIn Learning","name":"Excluded"}'
        b"]\n}"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        calls[request.url.path] += 1
        if request.url.path == "/authenticate":
            return response(request, 200, "shared-token")
        if request.url.path == "/courses":
            assert request.headers["Authorization"] == "shared-token"
            assert request.url.params["_filter"] == "vendor ne 'LinkedIn Learning'"
            offset = int(request.url.params["_offset"])
            if offset == 0:
                return httpx.Response(
                    200,
                    content=first_raw_payload,
                    headers={"Content-Type": "application/json"},
                    request=request,
                )
            return response(
                request,
                200,
                {
                    "totalItems": 3,
                    "returnedItems": 1,
                    "courses": [{"id": "c2", "vendor": "Other"}],
                },
            )
        if request.url.path in {"/courses/c1/enrollments", "/courses/c2/enrollments"}:
            return response(
                request,
                200,
                {"totalItems": 0, "returnedItems": 0, "enrollments": []},
            )
        raise AssertionError(request.url)

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.course_catalog_records == 3
    assert summary.courses_succeeded == 2
    assert calls["/authenticate"] == 1
    assert calls["/courses"] == 2
    assert calls["/courses/c1/enrollments"] == 1
    assert calls["/courses/c2/enrollments"] == 1
    assert calls["/courses/linkedin/enrollments"] == 0
    first_catalog = next(
        path
        for path in settings.bronze_local_path.rglob("offset=000000.json")  # type: ignore[attr-defined]
        if "course_catalog" in str(path)
    )
    assert first_catalog.read_bytes() == first_raw_payload
    stored = json.loads(first_catalog.read_text())
    assert [course["id"] for course in stored["courses"]] == ["c1", "linkedin"]


@pytest.mark.asyncio
async def test_enrollment_pagination_and_empty_course_are_successful(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()
    offsets: list[tuple[str, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        if request.url.path == "/courses":
            return response(
                request,
                200,
                {
                    "totalItems": 2,
                    "returnedItems": 2,
                    "courses": [{"id": "with-data"}, {"id": "empty"}],
                },
            )
        course_id = request.url.path.split("/")[2]
        offset = int(request.url.params["_offset"])
        offsets.append((course_id, offset))
        if course_id == "empty":
            return response(
                request, 200, {"totalItems": 0, "returnedItems": 0, "enrollments": []}
            )
        if offset == 0:
            return response(
                request,
                200,
                {
                    "totalItems": 3,
                    "returnedItems": 2,
                    "enrollments": [{"id": "e1"}, {"id": "e2"}],
                },
            )
        return response(
            request,
            200,
            {
                "totalItems": 3,
                "returnedItems": 1,
                "enrollments": [{"id": "e3"}],
            },
        )

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert summary.status == RunStatus.SUCCEEDED
    assert summary.enrollment_records == 3
    assert summary.courses_succeeded == 2
    assert sorted(offsets) == [("empty", 0), ("with-data", 0), ("with-data", 2)]


@pytest.mark.asyncio
async def test_course_concurrency_never_exceeds_setting(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory(levelup_page_size=10, levelup_max_concurrency=2)
    active = 0
    maximum_active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        if request.url.path == "/courses":
            courses = [{"id": f"c{index}"} for index in range(6)]
            return response(
                request,
                200,
                {"totalItems": 6, "returnedItems": 6, "courses": courses},
            )
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return response(
            request, 200, {"totalItems": 0, "returnedItems": 0, "enrollments": []}
        )

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert summary.status == RunStatus.SUCCEEDED
    assert summary.courses_succeeded == 6
    assert maximum_active == 2


@pytest.mark.asyncio
async def test_one_course_failure_does_not_stop_other_courses(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()
    called: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        if request.url.path == "/courses":
            return response(
                request,
                200,
                {
                    "totalItems": 2,
                    "returnedItems": 2,
                    "courses": [{"id": "bad"}, {"id": "good"}],
                },
            )
        called.append(request.url.path)
        if "/bad/" in request.url.path:
            return response(request, 404, {"error": "missing"})
        return response(
            request, 200, {"totalItems": 0, "returnedItems": 0, "enrollments": []}
        )

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert summary.courses_succeeded == 1
    assert summary.courses_failed == 1
    assert sorted(called) == ["/courses/bad/enrollments", "/courses/good/enrollments"]
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    await store.initialize()
    assert await store.find_resumable_run("levelup", 60) is None


@pytest.mark.asyncio
async def test_request_failure_does_not_create_raw_page(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        return response(request, 500, {"error": "unavailable"})

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    assert summary.status == RunStatus.FAILED
    assert list(settings.bronze_local_path.rglob("offset=*.json")) == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_resume_uses_checkpoint_without_refetching_catalog(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()
    run_id = "11111111-1111-4111-8111-111111111111"
    first_calls: Counter[str] = Counter()

    def first_handler(request: httpx.Request) -> httpx.Response:
        first_calls[request.url.path] += 1
        if request.url.path == "/authenticate":
            return response(request, 200, "token-1")
        if request.url.path == "/courses":
            return response(
                request,
                200,
                {"totalItems": 1, "returnedItems": 1, "courses": [{"id": "c1"}]},
            )
        offset = int(request.url.params["_offset"])
        if offset == 0:
            return response(
                request,
                200,
                {
                    "totalItems": 4,
                    "returnedItems": 2,
                    "enrollments": [{"id": "e1"}, {"id": "e2"}],
                },
            )
        return response(request, 500, {"error": "temporary"})

    first = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        run_id=run_id,
        transport=httpx.MockTransport(first_handler),
        sleep=no_sleep,
    )
    assert first.status == RunStatus.PARTIAL_FAILURE
    assert first.enrollment_records == 2

    second_calls: Counter[str] = Counter()
    resumed_offsets: list[int] = []

    def second_handler(request: httpx.Request) -> httpx.Response:
        second_calls[request.url.path] += 1
        if request.url.path == "/authenticate":
            return response(request, 200, "token-2")
        if request.url.path == "/courses":
            raise AssertionError("catalog must not be fetched during resume")
        resumed_offsets.append(int(request.url.params["_offset"]))
        return response(
            request,
            200,
            {
                "totalItems": 4,
                "returnedItems": 2,
                "enrollments": [{"id": "e3"}, {"id": "e4"}],
            },
        )

    second = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(second_handler),
        sleep=no_sleep,
    )
    assert second.status == RunStatus.SUCCEEDED
    assert second.run_id == run_id
    assert second.course_catalog_records == 1
    assert second.enrollment_records == 4
    assert second.courses_succeeded == 1
    assert second.courses_failed == 0
    assert resumed_offsets == [2]
    assert second_calls["/courses"] == 0
