from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from app.mocks.levelup import course_payload, enrollment_payload
from app.models import RunStatus
from app.repositories import CheckpointStore
from app.services.levelup.service import run_levelup_ingestion
from tests.conftest import no_sleep, response


def course(course_id: str, vendor: str | None = "LevelUP") -> dict[str, object]:
    return course_payload(course_id, f"Course {course_id}", vendor)


def enrollment(enrollment_id: str, course_id: str) -> dict[str, object]:
    return enrollment_payload(enrollment_id, course_id, f"user-{enrollment_id}")


def course_page(
    courses: list[dict[str, object]],
    *,
    total: int | None = None,
    limit: int = 2,
    offset: int = 0,
) -> dict[str, object]:
    return {
        "totalItems": len(courses) if total is None else total,
        "returnedItems": len(courses),
        "limit": limit,
        "offset": offset,
        "courses": courses,
    }


def enrollment_page(
    enrollments: list[dict[str, object]],
    *,
    total: int | None = None,
    limit: int = 2,
    offset: int = 0,
) -> dict[str, object]:
    return {
        "totalItems": len(enrollments) if total is None else total,
        "returnedItems": len(enrollments),
        "limit": limit,
        "offset": offset,
        "enrollments": enrollments,
    }


@pytest.mark.asyncio
async def test_catalog_pagination_filter_and_course_list_reuse(
    settings_factory: Callable[..., object], tmp_path: Path
) -> None:
    settings = settings_factory()
    calls: Counter[str] = Counter()
    first_page = course_page(
        [course("c1", None), course("linkedin", "LinkedIn Learning")],
        total=3,
    )
    first_raw_payload = json.dumps(first_page, indent=2).encode() + b"\n"

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
                course_page([course("c2", "Other")], total=3, offset=2),
            )
        if request.url.path in {"/courses/c1/enrollments", "/courses/c2/enrollments"}:
            return response(
                request,
                200,
                enrollment_page([], offset=0),
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
async def test_second_run_pulls_only_levelup_changes_and_keeps_failed_watermark(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    initial_course = course("c1")
    initial_course["dateEdited"] = "2026-08-24T04:00:00Z"
    initial_enrollment = enrollment("e1", "c1")
    initial_enrollment["dateEdited"] = "2026-08-24T04:30:00Z"

    def initial_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token-1")
        assert request.url.params["_sort"] == "dateEdited"
        if request.url.path == "/courses":
            assert request.url.params["_filter"] == "vendor ne 'LinkedIn Learning'"
            return response(request, 200, course_page([initial_course]))
        assert "_filter" not in request.url.params
        return response(request, 200, enrollment_page([initial_enrollment]))

    first = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(initial_handler),
        sleep=no_sleep,
    )
    assert first.status == RunStatus.SUCCEEDED

    changed_enrollment = enrollment("e2", "c1")
    changed_enrollment["dateEdited"] = "2026-08-25T05:00:00Z"

    def incremental_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token-2")
        assert request.url.params["_sort"] == "dateEdited"
        if request.url.path == "/courses":
            assert request.url.params["_filter"] == (
                "vendor ne 'LinkedIn Learning' and "
                "dateEdited gt '2026-08-24T04:00:00Z'"
            )
            return response(request, 200, course_page([]))
        assert request.url.params["_filter"] == (
            "dateEdited gt '2026-08-24T04:30:00Z'"
        )
        return response(request, 200, enrollment_page([changed_enrollment]))

    second = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(incremental_handler),
        sleep=no_sleep,
    )
    assert second.status == RunStatus.SUCCEEDED
    assert second.course_catalog_records == 0
    assert second.enrollment_records == 1
    second_enrollment_file = next(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            f"levelup/learning_history/**/run_id={second.run_id}/**/offset=*.json"
        )
    )
    assert [
        item["id"] for item in json.loads(second_enrollment_file.read_text())["enrollments"]
    ] == ["e2"]

    def failed_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token-3")
        if request.url.path == "/courses":
            return response(request, 200, course_page([]))
        assert request.url.params["_filter"] == (
            "dateEdited gt '2026-08-25T05:00:00Z'"
        )
        return response(request, 500, {"error": "temporary"})

    third = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(failed_handler),
        sleep=no_sleep,
    )
    assert third.status == RunStatus.PARTIAL_FAILURE
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    assert (
        await store.get_watermark("levelup", "learning_history", "c1")
        == "2026-08-25T05:00:00Z"
    )


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
                course_page([course("with-data"), course("empty")]),
            )
        course_id = request.url.path.split("/")[2]
        offset = int(request.url.params["_offset"])
        offsets.append((course_id, offset))
        if course_id == "empty":
            return response(
                request,
                200,
                enrollment_page([], offset=offset),
            )
        if offset == 0:
            return response(
                request,
                200,
                enrollment_page(
                    [enrollment("e1", course_id), enrollment("e2", course_id)],
                    total=3,
                    offset=offset,
                ),
            )
        return response(
            request,
            200,
            enrollment_page(
                [enrollment("e3", course_id)],
                total=3,
                offset=offset,
            ),
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
            courses = [course(f"c{index}") for index in range(6)]
            return response(
                request,
                200,
                course_page(courses, limit=10),
            )
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return response(
            request,
            200,
            enrollment_page([], limit=10),
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
                course_page([course("bad"), course("good")]),
            )
        called.append(request.url.path)
        if "/bad/" in request.url.path:
            return response(request, 404, {"error": "missing"})
        return response(
            request,
            200,
            enrollment_page([]),
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
async def test_next_ingestion_starts_new_run_after_failure(
    settings_factory: Callable[..., object]
) -> None:
    settings = settings_factory()
    first_calls: Counter[str] = Counter()

    def first_handler(request: httpx.Request) -> httpx.Response:
        first_calls[request.url.path] += 1
        if request.url.path == "/authenticate":
            return response(request, 200, "token-1")
        if request.url.path == "/courses":
            return response(
                request,
                200,
                course_page([course("c1")], total=1),
            )
        offset = int(request.url.params["_offset"])
        if offset == 0:
            return response(
                request,
                200,
                enrollment_page(
                    [enrollment("e1", "c1"), enrollment("e2", "c1")],
                    total=4,
                    offset=offset,
                ),
            )
        return response(request, 500, {"error": "temporary"})

    first = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(first_handler),
        sleep=no_sleep,
    )
    assert first.status == RunStatus.PARTIAL_FAILURE
    assert first.enrollment_records == 2

    second_calls: Counter[str] = Counter()
    second_offsets: list[int] = []

    def second_handler(request: httpx.Request) -> httpx.Response:
        second_calls[request.url.path] += 1
        if request.url.path == "/authenticate":
            return response(request, 200, "token-2")
        if request.url.path == "/courses":
            return response(
                request,
                200,
                course_page([course("c1")], total=1),
            )
        second_offsets.append(int(request.url.params["_offset"]))
        return response(
            request,
            200,
            enrollment_page(
                [enrollment("e3", "c1"), enrollment("e4", "c1")],
                total=4,
                offset=second_offsets[-1],
            ),
        )

    second = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(second_handler),
        sleep=no_sleep,
    )
    assert second.status == RunStatus.SUCCEEDED
    assert second.run_id != first.run_id
    assert second.course_catalog_records == 1
    assert second.enrollment_records == 4
    assert second.courses_succeeded == 1
    assert second.courses_failed == 0
    assert second_offsets == [0, 2]
    assert second_calls["/courses"] == 1


@pytest.mark.asyncio
async def test_missing_required_course_field_does_not_enter_bronze_and_fails_run(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    invalid_course = course("broken")
    del invalid_course["name"]
    raw_payload = json.dumps(course_page([invalid_course])).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        assert request.url.path == "/courses"
        return httpx.Response(
            200,
            content=raw_payload,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert summary.error_message is not None
    assert "courses.0.name:missing" in summary.error_message
    assert not list(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            "levelup/course_catalog/**/offset=*.json"
        )
    )


@pytest.mark.asyncio
async def test_wrong_enrollment_field_type_skips_bronze_without_logging_pii(
    settings_factory: Callable[..., object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = settings_factory()
    invalid_enrollment = enrollment("bad", "c1")
    invalid_enrollment["status"] = "COMPLETED"
    invalid_enrollment["fullName"] = "Sensitive Learner Name"
    raw_payload = json.dumps(enrollment_page([invalid_enrollment])).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        if request.url.path == "/courses":
            return response(request, 200, course_page([course("c1")]))
        return httpx.Response(
            200,
            content=raw_payload,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    caplog.set_level(logging.ERROR)
    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert "enrollments.0.status:int_type" in caplog.text
    assert "Sensitive Learner Name" not in caplog.text
    assert not list(
        settings.bronze_local_path.glob(  # type: ignore[attr-defined]
            "levelup/learning_history/**/offset=*.json"
        )
    )


@pytest.mark.asyncio
async def test_new_vendor_field_warns_but_does_not_fail(
    settings_factory: Callable[..., object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = settings_factory()
    expanded_course = course("c1")
    expanded_course["newVendorField"] = "new-value-must-not-be-logged"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/authenticate":
            return response(request, 200, "token")
        if request.url.path == "/courses":
            return response(request, 200, course_page([expanded_course]))
        return response(request, 200, enrollment_page([]))

    caplog.set_level(logging.WARNING)
    summary = await run_levelup_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert "courses.0.newVendorField" in caplog.text
    assert "new-value-must-not-be-logged" not in caplog.text
