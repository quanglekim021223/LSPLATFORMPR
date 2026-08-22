from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

app = FastAPI(title="Mock LevelUP API")

_API_KEY = "mock-private-key"
_TOKEN = "mock-token"
_COURSES = [
    {"id": "python-basic", "vendor": "LevelUP", "name": "Python Basic"},
    {
        "id": "linkedin-course",
        "vendor": "LinkedIn Learning",
        "name": "LinkedIn Course",
    },
    {
        "id": "data-engineering",
        "vendor": "LevelUP",
        "name": "Data Engineering",
    },
]
_ENROLLMENTS = {
    "python-basic": [
        {"id": "e1", "userId": "user-01"},
        {"id": "e2", "userId": "user-02"},
        {"id": "e3", "userId": "user-03"},
    ],
    "linkedin-course": [{"id": "ignored", "userId": "user-04"}],
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


@app.post("/authenticate")
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


@app.get("/courses")
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
        "courses": page,
    }


@app.get("/courses/{course_id}/enrollments")
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
        "enrollments": page,
    }
