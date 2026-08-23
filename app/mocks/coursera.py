from __future__ import annotations

import base64
from typing import Annotated, Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

router = APIRouter(tags=["Coursera"])

_USERNAME = "mock-coursera-user"
_PASSWORD = "mock-coursera-password"
_TOKEN = "mock-coursera-token"
_ORG_ID = "mock-org"
_CONTENTS = [
    {"contentId": "course-1", "name": "Python"},
    {"contentId": "course-2", "name": "SQL"},
    {"contentId": "course-3", "name": "Data Engineering"},
]
_ENROLLMENTS = [
    {"id": "enrollment-1", "contentId": "course-1"},
    {"id": "enrollment-2", "contentId": "course-2"},
    {"id": "enrollment-3", "contentId": "course-3"},
]


def _validate_bearer(authorization: str | None) -> None:
    if authorization != f"Bearer {_TOKEN}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")


def _validate_org(org_id: str) -> None:
    if org_id != _ORG_ID:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock organization")


def _page(records: list[dict[str, Any]], start: int, limit: int) -> dict[str, Any]:
    elements = records[start : start + limit]
    next_offset = start + limit
    paging: dict[str, Any] = {"total": len(records)}
    if next_offset < len(records):
        paging["next"] = next_offset
    return {"elements": elements, "paging": paging}


@router.post("/oauth2/client_credentials/token")
async def token(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str | int]:
    expected = base64.b64encode(f"{_USERNAME}:{_PASSWORD}".encode()).decode()
    form = parse_qs((await request.body()).decode())
    if (
        authorization != f"Basic {expected}"
        or form.get("grant_type") != ["client_credentials"]
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return {"access_token": _TOKEN, "token_type": "Bearer", "expires_in": 3600}


@router.get("/{org_id}/contents")
async def contents(
    org_id: str,
    start: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1)] = 100,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_org(org_id)
    _validate_bearer(authorization)
    return _page(_CONTENTS, start, limit)


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
    return {"elements": matches}


@router.get("/{org_id}/enrollmentReports")
async def enrollment_reports(
    org_id: str,
    start: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1)] = 100,
    include_s12n: Annotated[bool, Query(alias="includeS12n")] = True,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    del include_s12n
    _validate_org(org_id)
    _validate_bearer(authorization)
    return _page(_ENROLLMENTS, start, limit)
