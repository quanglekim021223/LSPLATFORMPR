from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.mocks.generated_data import generated_vendor_data
from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["LevelUP"])
_DATE_EDITED_FILTER = re.compile(r"dateEdited gt '([^']+)'")


def course_payload(
    course_id: str, name: str, vendor: str | None
) -> dict[str, object]:
    return {
        "id": course_id,
        "courseType": "OnlineCourse",
        "name": name,
        "description": f"<p>{name}</p>",
        "notes": None,
        "externalId": None,
        "accessDate": None,
        "expireType": 0,
        "expireDuration": {"years": 0, "months": 0, "days": 0, "hours": 0},
        "expiryDate": None,
        "activeStatus": 0,
        "tagIds": [],
        "resourceIds": [],
        "editorIds": [],
        "prices": [],
        "competencyDefinitionIds": [],
        "prerequisiteCourseIds": [],
        "postEnrollmentCourseIds": [],
        "allowCourseEvaluation": True,
        "categoryId": "mock-category",
        "certificateUrl": None,
        "audience": None,
        "goals": None,
        "vendor": vendor,
        "companyCost": None,
        "learnerCost": None,
        "companyTime": None,
        "learnerTime": None,
        "dateEdited": "2026-08-24T04:00:00",
        "dateAdded": "2026-08-20T04:00:00",
    }


def enrollment_payload(
    enrollment_id: str, course_id: str, user_id: str
) -> dict[str, object]:
    return {
        "id": enrollment_id,
        "courseId": course_id,
        "courseName": "Mock course",
        "progress": 100.0,
        "score": 90.0,
        "status": 3,
        "dateCompleted": "2026-08-24T04:30:00",
        "dateExpires": None,
        "fullName": f"Mock Learner {user_id}",
        "jobTitle": "Software Engineer",
        "courseVersionId": None,
        "userId": user_id,
        "acceptedTermsAndConditions": False,
        "timeSpent": "00:30:00",
        "dateStarted": "2026-08-24T04:00:00",
        "enrollmentKeyId": None,
        "certificateId": None,
        "credits": None,
        "isActive": True,
        "dateDue": None,
        "dateEdited": "2026-08-24T04:30:00",
        "dateAdded": "2026-08-24T04:00:00",
    }


_COURSES = [
    course_payload("python-basic", "Python Basic", "LevelUP"),
    course_payload("linkedin-course", "LinkedIn Course", "LinkedIn Learning"),
    course_payload("data-engineering", "Data Engineering", "LevelUP"),
]
_ENROLLMENTS = {
    "python-basic": [
        enrollment_payload("e1", "python-basic", "user-01"),
        enrollment_payload("e2", "python-basic", "user-02"),
        enrollment_payload("e3", "python-basic", "user-03"),
    ],
    "linkedin-course": [
        enrollment_payload("ignored", "linkedin-course", "user-04")
    ],
    "data-engineering": [],
}

_GENERATED = generated_vendor_data("levelup")
if _GENERATED is not None:
    _COURSES = _GENERATED["courses"]
    _ENROLLMENTS = _GENERATED["enrollments"]


class AuthenticationRequest(BaseModel):
    username: str
    password: str
    private_key: str = Field(alias="privateKey")


def _validate_client(api_key: str | None, api_version: str | None) -> None:
    settings = get_mock_settings()
    if (
        api_key != settings.mock_levelup_api_key.get_secret_value()
        or api_version != settings.mock_levelup_api_version
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock API credentials")


def _validate_token(
    authorization: str | None, api_key: str | None, api_version: str | None
) -> None:
    _validate_client(api_key, api_version)
    if authorization != get_mock_settings().mock_levelup_access_token.get_secret_value():
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")


def _apply_incremental_filter(
    items: list[dict[str, object]], filter_value: str | None
) -> list[dict[str, object]]:
    match = _DATE_EDITED_FILTER.search(filter_value or "")
    if match is None:
        return items
    watermark = _parse_timestamp(match.group(1))
    return [
        item
        for item in items
        if isinstance(item.get("dateEdited"), str)
        and _parse_timestamp(str(item["dateEdited"])) > watermark
    ]


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@router.post("/authenticate")
async def authenticate(
    credentials: AuthenticationRequest,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> str:
    settings = get_mock_settings()
    _validate_client(api_key, api_version)
    if (
        credentials.username != settings.mock_levelup_username.get_secret_value()
        or credentials.password != settings.mock_levelup_password.get_secret_value()
        or credentials.private_key
        != settings.mock_levelup_api_key.get_secret_value()
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock login")
    return settings.mock_levelup_access_token.get_secret_value()


@router.get("/courses")
async def courses(
    limit: Annotated[int, Query(alias="_limit", ge=1)] = 1000,
    offset: Annotated[int, Query(alias="_offset", ge=0)] = 0,
    filter_value: Annotated[str | None, Query(alias="_filter")] = None,
    sort_value: Annotated[str | None, Query(alias="_sort")] = None,
    authorization: Annotated[str | None, Header()] = None,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> dict[str, object]:
    _validate_token(authorization, api_key, api_version)
    filtered = _apply_incremental_filter(_COURSES, filter_value)
    if sort_value == "dateEdited":
        filtered = sorted(filtered, key=lambda item: str(item["dateEdited"]))
    page = filtered[offset : offset + limit]
    return {
        "totalItems": len(filtered),
        "returnedItems": len(page),
        "limit": limit,
        "offset": offset,
        "courses": page,
    }


@router.get("/courses/{course_id}/enrollments")
async def enrollments(
    course_id: str,
    limit: Annotated[int, Query(alias="_limit", ge=1)] = 1000,
    offset: Annotated[int, Query(alias="_offset", ge=0)] = 0,
    filter_value: Annotated[str | None, Query(alias="_filter")] = None,
    sort_value: Annotated[str | None, Query(alias="_sort")] = None,
    authorization: Annotated[str | None, Header()] = None,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> dict[str, object]:
    _validate_token(authorization, api_key, api_version)
    if course_id not in _ENROLLMENTS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock course")
    all_enrollments = _apply_incremental_filter(
        _ENROLLMENTS[course_id],
        filter_value,
    )
    if sort_value == "dateEdited":
        all_enrollments = sorted(
            all_enrollments,
            key=lambda item: str(item["dateEdited"]),
        )
    page = all_enrollments[offset : offset + limit]
    return {
        "totalItems": len(all_enrollments),
        "returnedItems": len(page),
        "limit": limit,
        "offset": offset,
        "enrollments": page,
    }
