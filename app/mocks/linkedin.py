from __future__ import annotations

from typing import Annotated, Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

router = APIRouter(tags=["LinkedIn Learning"])

_CLIENT_ID = "mock-linkedin-client"
_CLIENT_SECRET = "mock-linkedin-secret"
_TOKEN = "mock-linkedin-token"
_ASSETS = [
    {"urn": "urn:li:lyndaCourse:1", "title": "Python"},
    {"urn": "urn:li:lyndaCourse:2", "title": "SQL"},
    {"urn": "urn:li:lyndaCourse:3", "title": "Data Engineering"},
]


def _validate_bearer(authorization: str | None) -> None:
    if authorization != f"Bearer {_TOKEN}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")


def _page(
    records: list[dict[str, Any]], start: int, count: int, endpoint: str
) -> dict[str, Any]:
    elements = records[start : start + count]
    next_offset = start + count
    links: list[dict[str, str]] = []
    if next_offset < len(records):
        links.append(
            {
                "rel": "next",
                "href": f"/v2/{endpoint}?start={next_offset}&count={count}",
                "type": "application/json",
            }
        )
    return {
        "elements": elements,
        "paging": {
            "total": len(records),
            "count": count,
            "start": start,
            "links": links,
        },
    }


@router.post("/oauth/v2/accessToken")
async def token(request: Request) -> dict[str, str | int]:
    form = parse_qs((await request.body()).decode())
    if form != {
        "grant_type": ["client_credentials"],
        "client_id": [_CLIENT_ID],
        "client_secret": [_CLIENT_SECRET],
    }:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return {"access_token": _TOKEN, "expires_in": 3600}


@router.get("/learningAssets")
async def learning_assets(
    q: Annotated[str, Query()] = "criteria",
    start: Annotated[int, Query(ge=0)] = 0,
    count: Annotated[int, Query(ge=1, le=100)] = 100,
    asset_urn: Annotated[
        str | None, Query(alias="assetFilteringCriteria.urn")
    ] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_bearer(authorization)
    if q != "criteria":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "q must be criteria")
    if asset_urn is not None:
        matches = [asset for asset in _ASSETS if asset["urn"] == asset_urn]
        return _page(matches, 0, count, "learningAssets")
    return _page(_ASSETS, start, count, "learningAssets")


@router.get("/learningActivityReports")
async def activity_reports(
    q: Annotated[str, Query()],
    started_at: Annotated[int, Query(alias="startedAt")],
    duration: Annotated[int, Query(alias="timeOffset.duration", ge=1, le=14)],
    unit: Annotated[str, Query(alias="timeOffset.unit")],
    start: Annotated[int, Query(ge=0)] = 0,
    count: Annotated[int, Query(ge=1, le=100)] = 100,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_bearer(authorization)
    if q != "criteria" or unit != "DAY":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Invalid criteria")
    records = [
        {
            "id": f"activity-{started_at}",
            "startedAt": started_at,
            "durationDays": duration,
        }
    ]
    return _page(records, start, count, "learningActivityReports")
