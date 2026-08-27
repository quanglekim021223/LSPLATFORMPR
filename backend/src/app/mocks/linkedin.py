from __future__ import annotations

from typing import Annotated, Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["LinkedIn Learning"])

def token_payload(token_value: str | None = None) -> dict[str, str | int]:
    token = token_value or get_mock_settings().mock_linkedin_access_token.get_secret_value()
    return {"access_token": token, "expires_in": 7_776_000}


def _localized(value: str) -> dict[str, Any]:
    return {"locale": {"country": "US", "language": "en"}, "value": value}


def asset_payload(urn: str, title: str) -> dict[str, Any]:
    localized_title = _localized(title)
    return {
        "urn": urn,
        "details": {
            "images": {"primary": "https://example.test/course.jpg"},
            "descriptionIncludingHtml": _localized(f"<p>{title}</p>"),
            "lastUpdatedAt": 1_787_122_740_000,
            "publishedAt": 1_700_000_000_000,
            "discoverableBy": [
                {
                    "accessor": {
                        "name": _localized("FPT Software"),
                        "urn": "urn:li:enterpriseAccount:1",
                    }
                }
            ],
            "description": _localized(f"Description for {title}"),
            "shortDescription": _localized(title),
            "availability": "AVAILABLE",
            "availableLocales": [{"country": "US", "language": "en"}],
            "relationships": [],
            "classifications": [],
            "urls": {
                "ssoLaunch": "https://example.test/sso",
                "webLaunch": "https://example.test/web",
                "aiccLaunch": "https://example.test/aicc",
            },
            "shortDescriptionIncludingHtml": _localized(f"<p>{title}</p>"),
            "contributors": [],
            "timeToComplete": {"duration": 3600, "unit": "SECOND"},
        },
        "title": localized_title,
        "type": "COURSE",
        "contents": [],
    }


def activity_report_payload(index: int, started_at: int) -> dict[str, Any]:
    return {
        "latestDataAt": started_at,
        "learnerDetails": {
            "name": f"Learner {index}",
            "enterpriseGroups": ["OFFICIAL"],
            "entity": {
                "profileUrn": (
                    "urn:li:enterpriseProfile:(urn:li:enterpriseAccount:1,"
                    f"{index})"
                )
            },
            "email": f"learner{index}@example.test",
            "customAttributes": {},
            "uniqueUserId": f"learner{index}@example.test",
        },
        "activities": [
            {
                "engagementType": "SECONDS_VIEWED",
                "lastEngagedAt": started_at,
                "firstEngagedAt": started_at,
                "assetType": "COURSE",
                "engagementMetricQualifier": "TOTAL",
                "engagementValue": index,
            }
        ],
        "contentDetails": {
            "name": f"Course {index}",
            "contentProviderName": "LinkedIn",
            "contentUrn": f"urn:li:lyndaCourse:{index}",
            "locale": {"country": "US", "language": "en"},
        },
    }


_ASSETS = [
    asset_payload("urn:li:lyndaCourse:1", "Python"),
    asset_payload("urn:li:lyndaCourse:2", "SQL"),
    asset_payload("urn:li:lyndaCourse:3", "Data Engineering"),
]


def _validate_bearer(authorization: str | None) -> None:
    token = get_mock_settings().mock_linkedin_access_token.get_secret_value()
    if authorization != f"Bearer {token}":
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
    payload = {
        "elements": elements,
        "paging": {
            "total": len(records),
            "count": count,
            "start": start,
            "links": links,
        },
    }
    if endpoint == "learningAssets":
        payload["metadata"] = {}
    return payload


@router.post("/oauth/v2/accessToken")
async def token(request: Request) -> dict[str, str | int]:
    settings = get_mock_settings()
    form = parse_qs((await request.body()).decode())
    if form != {
        "grant_type": ["client_credentials"],
        "client_id": [settings.mock_linkedin_client_id.get_secret_value()],
        "client_secret": [settings.mock_linkedin_client_secret.get_secret_value()],
    }:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return token_payload()


@router.get("/learningAssets")
async def learning_assets(
    q: Annotated[str, Query()] = "criteria",
    start: Annotated[int, Query(ge=0)] = 0,
    count: Annotated[int, Query(ge=1, le=100)] = 100,
    asset_type: Annotated[
        str | None, Query(alias="assetFilteringCriteria.assetTypes[0]")
    ] = None,
    last_modified_after: Annotated[
        int | None, Query(alias="assetFilteringCriteria.lastModifiedAfter", ge=0)
    ] = None,
    include_retired: Annotated[
        bool, Query(alias="assetRetrievalCriteria.includeRetired")
    ] = False,
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
    if asset_type != "COURSE" or not include_retired:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "Catalog requires one COURSE type and includeRetired=true",
        )
    matches = _ASSETS
    if last_modified_after is not None:
        matches = [
            asset
            for asset in matches
            if asset["details"]["lastUpdatedAt"] > last_modified_after
        ]
    return _page(matches, start, count, "learningAssets")


@router.get("/learningActivityReports")
async def activity_reports(
    q: Annotated[str, Query()],
    primary_aggregation: Annotated[
        str, Query(alias="aggregationCriteria.primary")
    ],
    secondary_aggregation: Annotated[
        str, Query(alias="aggregationCriteria.secondary")
    ],
    asset_type: Annotated[str, Query(alias="assetType")],
    content_source: Annotated[str, Query(alias="contentSource")],
    started_at: Annotated[int, Query(alias="startedAt")],
    duration: Annotated[int, Query(alias="timeOffset.duration", ge=1, le=14)],
    unit: Annotated[str, Query(alias="timeOffset.unit")],
    start: Annotated[int, Query(ge=0)] = 0,
    count: Annotated[int, Query(ge=1, le=100)] = 100,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    _validate_bearer(authorization)
    if (
        q != "criteria"
        or primary_aggregation != "INDIVIDUAL"
        or secondary_aggregation != "CONTENT"
        or asset_type != "COURSE"
        or content_source != "LINKEDIN_LEARNING"
        or unit != "DAY"
    ):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Invalid criteria")
    records = [activity_report_payload(duration, started_at)]
    return _page(records, start, count, "learningActivityReports")
