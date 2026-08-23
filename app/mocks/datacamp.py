from __future__ import annotations

from math import ceil
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

router = APIRouter(tags=["DataCamp"])

_TOKEN = "mock-datacamp-token"
_LIVE_COURSES = {
    "data": [
        {"id": "dc-python", "title": "Introduction to Python"},
        {"id": "dc-sql", "title": "Introduction to SQL"},
    ]
}
_ARCHIVED_COURSES = {
    "data": [{"id": "dc-legacy-r", "title": "Legacy R"}]
}
_EVENTS = [
    {"id": "event-01", "type": "course_completed"},
    {"id": "event-02", "type": "chapter_completed"},
    {"id": "event-03", "type": "course_completed"},
]


def _validate_headers(authorization: str | None, accept: str | None) -> None:
    if authorization != f"Bearer {_TOKEN}" or accept != "application/json":
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
        "events": records,
        "meta": {
            "page": page,
            "numberOfPages": ceil(len(_EVENTS) / page_size),
        },
    }
