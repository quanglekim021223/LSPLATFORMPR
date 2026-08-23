from __future__ import annotations

from math import ceil
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

router = APIRouter(tags=["SkillUp"])

_API_KEY = "mock-skillup-key"
_TAXONOMY = [
    {"skillId": "python", "skillName": "Python"},
    {"skillId": "sql", "skillName": "SQL"},
    {"skillId": "data-engineering", "skillName": "Data Engineering"},
]
_SKILL_PROFILES = [
    {
        "employeeId": "employee-01",
        "employeeName": "An Nguyen",
        "skills": [{"skillId": "python", "proficiency": 4}],
    },
    {
        "employeeId": "employee-02",
        "employeeName": "Binh Tran",
        "skills": [{"skillId": "sql", "proficiency": 3}],
    },
    {
        "employeeId": "employee-03",
        "employeeName": "Chi Le",
        "skills": [{"skillId": "data-engineering", "proficiency": 5}],
    },
]
_REPORTS = [
    {"reportId": "report-01", "employeeId": "employee-01", "score": 84},
    {"reportId": "report-02", "employeeId": "employee-02", "score": 76},
    {"reportId": "report-03", "employeeId": "employee-03", "score": 92},
]


def _validate_api_key(api_key: str | None) -> None:
    if api_key != _API_KEY:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock API key")


def _page(
    records: list[dict[str, Any]], page_number: int, page_size: int
) -> tuple[list[dict[str, Any]], int]:
    start = (page_number - 1) * page_size
    return records[start : start + page_size], ceil(len(records) / page_size)


@router.get("/taxonomy")
async def taxonomy(
    page_number: Annotated[int, Query(alias="PageNumber", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="PageSize", ge=1, le=100)] = 100,
    api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> dict[str, Any]:
    _validate_api_key(api_key)
    items, total_pages = _page(_TAXONOMY, page_number, page_size)
    return {
        "items": items,
        "pageNumber": page_number,
        "pageSize": page_size,
        "totalItems": len(_TAXONOMY),
        "totalPages": total_pages,
        "hasNextPage": page_number < total_pages,
    }


@router.get("/employees/skills-profile")
async def skill_inventory(
    page_number: Annotated[int, Query(alias="pageNumber", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=100)] = 100,
    skill_profile_modified_since: Annotated[
        str | None, Query(alias="SkillProfileModifiedSince")
    ] = None,
    search_text: Annotated[str | None, Query(alias="searchText")] = None,
    api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> dict[str, Any]:
    del skill_profile_modified_since
    _validate_api_key(api_key)
    records = _SKILL_PROFILES
    if search_text:
        query = search_text.casefold()
        records = [
            record
            for record in records
            if query in str(record["employeeName"]).casefold()
        ]
    items, total_pages = _page(records, page_number, page_size)
    return {
        "items": items,
        "metadata": {
            "pageNumber": page_number,
            "pageSize": page_size,
            "totalItems": len(records),
            "totalPages": total_pages,
            "hasNextPage": page_number < total_pages,
        },
    }


@router.get("/v3/reports")
async def assessment_history(
    page_number: Annotated[int, Query(alias="PageNo", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="PageSize", ge=1, le=100)] = 100,
    include_sections: Annotated[bool | None, Query(alias="includeSections")] = None,
    start_date: Annotated[str | None, Query(alias="startDate")] = None,
    end_date: Annotated[str | None, Query(alias="endDate")] = None,
    api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> dict[str, Any]:
    del start_date, end_date
    _validate_api_key(api_key)
    reports, total_pages = _page(_REPORTS, page_number, page_size)
    if include_sections:
        reports = [
            {**report, "sections": [{"name": "Core Skills", "score": report["score"]}]}
            for report in reports
        ]
    return {
        "reports": reports,
        "pageNumber": page_number,
        "pageSize": page_size,
        "totalItems": len(_REPORTS),
        "totalPages": total_pages,
        "hasNextPage": page_number < total_pages,
    }
