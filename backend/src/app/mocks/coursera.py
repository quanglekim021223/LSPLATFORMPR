from __future__ import annotations

import base64
from typing import Annotated, Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["Coursera"])

def token_payload(token_value: str | None = None) -> dict[str, Any]:
    token = token_value or get_mock_settings().mock_coursera_access_token.get_secret_value()
    return {
        "token_type": "Bearer",
        "access_token": token,
        "grant_type": "client_credentials",
        "issued_at": 1787213698,
        "expires_in": 1799,
    }


def course_payload(content_id: str, name: str) -> dict[str, Any]:
    slug = name.lower().replace(" ", "-")
    return {
        "subtitleLanguageCodes": ["en", "vi"],
        "lastUpdatedAt": 1769778943,
        "difficultyLevel": "BEGINNER",
        "contentId": content_id,
        "description": f"Description for {name}",
        "languageCode": "en",
        "instructors": [
            {
                "photoUrl": "https://example.test/instructor.jpg",
                "name": "Coursera Instructor",
                "title": "Professor",
                "department": "Computer Science",
            }
        ],
        "partners": [
            {
                "name": "Coursera Partner",
                "logoUrl": "https://example.test/partner.png",
            }
        ],
        "name": name,
        "programs": [
            {
                "contentUrl": f"https://example.test/learn/{slug}",
                "programId": "mock-program",
            }
        ],
        "id": f"Course~{content_id}",
        "extraMetadata": {
            "typeName": "courseMetadata",
            "definition": {
                "skills": [{"skillName": "Python", "skillId": "python"}],
                "estimatedLearningTime": 3600,
                "promoPhoto": "https://example.test/course.png",
                "domainTypes": [
                    {
                        "domain": {"name": "Technology", "domainId": "technology"},
                        "subdomain": {
                            "name": "Software Development",
                            "subdomainId": "software-development",
                        },
                    }
                ],
            },
        },
        "contentType": "Course",
        "slug": slug,
    }


def enrollment_payload(
    enrollment_id: str, content_id: str, *, completed: bool
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": enrollment_id,
        "programId": "mock-program",
        "externalId": "learner@example.test",
        "contentId": content_id,
        "contentType": "Course",
        "isCompleted": completed,
        "lastActivityAt": 1757911517000,
        "membershipState": "MEMBER",
        "enrolledAt": 1745199035000,
        "overallProgress": 100 if completed else 2,
        "approxTotalCourseHrs": 1.5,
        "updatedAt": 1787206959000,
        "contentName": f"Course {content_id}",
        "contentSlug": content_id,
        "partnerNames": ["Coursera Partner"],
        "fullName": "NGUYEN VAN A",
        "email": "learner@example.test",
        "programName": "Mock Learning Program",
        "programSlug": "mock-learning-program",
        "contractId": "mock-contract",
        "contractName": "Mock Licenses",
        "courseType": "Course",
    }
    if completed:
        payload.update(
            {
                "completedAt": 1750071584000,
                "grade": 0.977,
                "contentCertificateUrl": "https://example.test/certificate",
            }
        )
    return payload


_CONTENTS = [
    course_payload("course-1", "Python"),
    course_payload("course-2", "SQL"),
    course_payload("course-3", "Data Engineering"),
]
_ENROLLMENTS = [
    enrollment_payload("enrollment-1", "course-1", completed=True),
    enrollment_payload("enrollment-2", "course-2", completed=False),
    enrollment_payload("enrollment-3", "course-3", completed=True),
]


def _validate_bearer(authorization: str | None) -> None:
    token = get_mock_settings().mock_coursera_access_token.get_secret_value()
    if authorization != f"Bearer {token}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")


def _validate_org(org_id: str) -> None:
    if org_id != get_mock_settings().mock_coursera_org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock organization")


def _page(records: list[dict[str, Any]], start: int, limit: int) -> dict[str, Any]:
    elements = records[start : start + limit]
    next_offset = start + limit
    paging: dict[str, Any] = {"total": len(records)}
    if next_offset < len(records):
        paging["next"] = str(next_offset)
    return {"elements": elements, "paging": paging, "linked": {}}


@router.post("/oauth2/client_credentials/token")
async def token(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    settings = get_mock_settings()
    username = settings.mock_coursera_username.get_secret_value()
    password = settings.mock_coursera_password.get_secret_value()
    expected = base64.b64encode(f"{username}:{password}".encode()).decode()
    form = parse_qs((await request.body()).decode())
    if (
        authorization != f"Basic {expected}"
        or form.get("grant_type") != ["client_credentials"]
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return token_payload()


@router.get("/{org_id}/contents")
async def contents(
    org_id: str,
    start: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1)] = 100,
    modified_since_timestamp: Annotated[
        int | None, Query(alias="modifiedSinceTimestamp", ge=0)
    ] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_org(org_id)
    _validate_bearer(authorization)
    records = _CONTENTS
    if modified_since_timestamp is not None:
        records = [
            {**item, "changes": [{"changeType": "MODIFIED", "programIds": []}]}
            for item in records
            if item["lastUpdatedAt"] > modified_since_timestamp
        ]
    return _page(records, start, limit)


@router.get("/{org_id}/contents/{content_id}/detail")
async def content_detail(
    org_id: str,
    content_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_org(org_id)
    _validate_bearer(authorization)
    matches = [item for item in _CONTENTS if item["contentId"] == content_id]
    if not matches:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock content")
    return {"elements": matches, "paging": {}, "linked": {}}


@router.get("/{org_id}/enrollmentReports")
async def enrollment_reports(
    org_id: str,
    start: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1)] = 100,
    include_s12n: Annotated[bool, Query(alias="includeS12n")] = True,
    include_deleted_members: Annotated[
        bool, Query(alias="includeDeletedMembers")
    ] = False,
    include_expired_contracts: Annotated[
        bool, Query(alias="includeExpiredContracts")
    ] = False,
    last_activity_after: Annotated[
        int | None, Query(alias="lastActivityAfter", ge=0)
    ] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    del include_s12n, include_deleted_members, include_expired_contracts
    _validate_org(org_id)
    _validate_bearer(authorization)
    records = _ENROLLMENTS
    if last_activity_after is not None:
        records = [
            item
            for item in records
            if item["lastActivityAt"] > last_activity_after
        ]
    return _page(records, start, limit)
