from __future__ import annotations

from math import ceil
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

from app.mocks.generated_data import generated_vendor_data
from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["DataCamp"])


def course_payload(
    course_id: str,
    title: str,
    *,
    live: bool,
    technology: str | None = "Python",
) -> dict[str, Any]:
    slug = course_id.removeprefix("course-")
    return {
        "id": course_id,
        "title": title,
        "description": f"Description for {title}",
        "url": f"https://example.test/courses/{slug}/continue",
        "imageUrl": {
            "jpg": "https://example.test/images/course.jpg",
            "png": "https://example.test/images/course.png",
            "svg": "https://example.test/images/course.svg",
        },
        "technology": technology,
        "instructors": [{"fullName": "DataCamp Instructor"}] if live else [],
        "timeNeededInHours": 1,
        "topic": (
            {"name": "Programming", "description": None} if live else None
        ),
        "updatedAt": "2026-08-20T18:00:19.100Z",
        "live": live,
        "chapters": (
            [
                {
                    "id": f"course-chapter-{slug}",
                    "description": "Chapter description",
                    "title": "Chapter title",
                    "url": f"https://example.test/courses/{slug}/chapter",
                }
            ]
            if live
            else []
        ),
        "infoUrl": f"https://example.test/learn/{slug}",
        "publicInfoUrl": f"https://example.test/public/{slug}",
        "includedInLicenses": [],
    }


def event_payload(index: int) -> dict[str, Any]:
    return {
        "eventType": "completion" if index % 2 == 0 else "started",
        "contentId": f"course-{29000 + index}",
        "timestamp": f"2026-08-21T09:4{index}:00.000Z",
        "parentContentId": None,
        "user": {
            "email": f"learner-{index}@example.test",
            "nameid": f"LEARNER-{index}@EXAMPLE.TEST",
            "lmsUsername": None,
        },
        "assessmentScore": None,
        "knowledgeLevel": None,
    }


_LIVE_COURSES = {
    "data": [
        course_payload("course-54859", "Introduction to Python", live=True),
        course_payload("course-52942", "Introduction to SQL", live=True),
    ]
}
_ARCHIVED_COURSES = {
    "data": [
        course_payload(
            "course-1032", "Redacted DataCamp Course", live=False, technology=None
        )
    ]
}
_EVENTS = [event_payload(1), event_payload(2), event_payload(3)]

_GENERATED = generated_vendor_data("datacamp")
if _GENERATED is not None:
    _LIVE_COURSES = {"data": _GENERATED["live_courses"]}
    _ARCHIVED_COURSES = {"data": _GENERATED["archived_courses"]}
    _EVENTS = _GENERATED["events"]


def _validate_headers(authorization: str | None, accept: str | None) -> None:
    token = get_mock_settings().mock_datacamp_token.get_secret_value()
    if authorization != f"Bearer {token}" or accept != "application/json":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")


@router.get("/v1/catalog/live-courses")
async def live_courses(
    authorization: Annotated[str | None, Header()] = None,
    accept: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_headers(authorization, accept)
    return _LIVE_COURSES


@router.get("/v1/catalog/archived-courses")
async def archived_courses(
    authorization: Annotated[str | None, Header()] = None,
    accept: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_headers(authorization, accept)
    return _ARCHIVED_COURSES


@router.get("/v1/events")
async def events(
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=1000)] = 1000,
    content_type: Annotated[str | None, Query(alias="contentType")] = None,
    event_type: Annotated[str | None, Query(alias="eventType")] = None,
    from_value: Annotated[str | None, Query(alias="from")] = None,
    to: Annotated[str | None, Query()] = None,
    authorization: Annotated[str | None, Header()] = None,
    accept: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    del content_type, event_type, from_value, to
    _validate_headers(authorization, accept)
    start = (page - 1) * page_size
    records = _EVENTS[start : start + page_size]
    return {
        "data": records,
        "meta": {
            "page": page,
            "pageSize": page_size,
            "numberOfPages": ceil(len(_EVENTS) / page_size),
        },
    }
