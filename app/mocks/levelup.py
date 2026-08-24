from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

router = APIRouter(tags=["LevelUP"])

_API_KEY = "mock-private-key"
_TOKEN = "mock-token"


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


class AuthenticationRequest(BaseModel):
    username: str
    password: str
    private_key: str = Field(alias="privateKey")


def _validate_client(api_key: str | None, api_version: str | None) -> None:
    if api_key != _API_KEY or api_version != "2":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock API credentials")


def _validate_token(
    authorization: str | None, api_key: str | None, api_version: str | None
) -> None:
    _validate_client(api_key, api_version)
    if authorization != _TOKEN:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")


@router.post("/authenticate")
async def authenticate(
    credentials: AuthenticationRequest,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> str:
    _validate_client(api_key, api_version)
    if (
        credentials.username != "mock-user"
        or credentials.password != "mock-password"
        or credentials.private_key != _API_KEY
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock login")
    return _TOKEN


@router.get("/courses")
async def courses(
    limit: Annotated[int, Query(alias="_limit", ge=1)] = 1000,
    offset: Annotated[int, Query(alias="_offset", ge=0)] = 0,
    authorization: Annotated[str | None, Header()] = None,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> dict[str, object]:
    _validate_token(authorization, api_key, api_version)
    page = _COURSES[offset : offset + limit]
    return {
        "totalItems": len(_COURSES),
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
    authorization: Annotated[str | None, Header()] = None,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> dict[str, object]:
    _validate_token(authorization, api_key, api_version)
    if course_id not in _ENROLLMENTS:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock course")
    all_enrollments = _ENROLLMENTS[course_id]
    page = all_enrollments[offset : offset + limit]
    return {
        "totalItems": len(all_enrollments),
        "returnedItems": len(page),
        "limit": limit,
        "offset": offset,
        "enrollments": page,
    }
