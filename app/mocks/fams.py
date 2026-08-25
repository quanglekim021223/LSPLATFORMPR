from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["FAMS"])
_CLASSES = [
    {
        "classId": "class-001",
        "status": "CLOSED",
        "site": "HCM",
        "actualStartDate": "20260820",
    },
    {
        "classId": "class-002",
        "status": "INPROGRESS",
        "site": "HN",
        "actualStartDate": "20260823",
    },
]
_STUDENTS = [
    {"studentId": "student-001", "classId": "class-001"},
    {"studentId": "student-002", "classId": "class-001"},
    {"studentId": "student-003", "classId": "class-002"},
]


@router.get("/api/fsa-reports/training-data")
async def training_data(
    api_key: Annotated[str | None, Header(alias="Fsa-Report-Api-Key")] = None,
    accept: Annotated[str | None, Header()] = None,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
    site: Annotated[str | None, Query()] = None,
    actual_start_date_from: Annotated[
        str | None,
        Query(alias="actualStartDateFrom"),
    ] = None,
    actual_start_date_to: Annotated[
        str | None,
        Query(alias="actualStartDateTo"),
    ] = None,
) -> dict[str, Any]:
    expected_key = get_mock_settings().mock_fams_api_key.get_secret_value()
    if api_key != expected_key or accept != "application/json":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid FAMS API key")

    statuses = set(status_filter.split(",")) if status_filter else None
    classes = [
        item
        for item in _CLASSES
        if (statuses is None or item["status"] in statuses)
        and (site is None or item["site"] == site)
        and (
            actual_start_date_from is None
            or item["actualStartDate"] >= actual_start_date_from
        )
        and (
            actual_start_date_to is None
            or item["actualStartDate"] <= actual_start_date_to
        )
    ]
    class_ids = {item["classId"] for item in classes}
    students = [item for item in _STUDENTS if item["classId"] in class_ids]
    return {
        "success": True,
        "message": "Mock FAMS training data",
        "error_code": "",
        "data": {
            "classList": classes,
            "studentList": students,
        },
    }
