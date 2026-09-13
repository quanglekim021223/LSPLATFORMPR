from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

from tests.support.mocks.generated_data import generated_vendor_data
from tests.support.mocks.settings import get_mock_settings

router = APIRouter(tags=["FAMS"])
_FAMS_COURSE_1_NAME = "FAMS Course 1"

_CLASSES: list[dict[str, Any]] = [
    {
        "id": 1,
        "site": "HCM",
        "courseCode": "class-001",
        "courseName": _FAMS_COURSE_1_NAME,
        "courseStatus": "CLOSED",
        "actualStartDate": "2026-08-20",
    },
    {
        "id": 2,
        "site": "HN",
        "courseCode": "class-002",
        "courseName": "FAMS Course 2",
        "courseStatus": "INPROGRESS",
        "actualStartDate": "2026-08-23",
    },
]
_STUDENTS: list[dict[str, Any]] = [
    {
        "account": "student-001",
        "name": "Student 1",
        "site": "HCM",
        "courseCode": "class-001",
        "courseName": _FAMS_COURSE_1_NAME,
        "statusInClass": "Graduated",
    },
    {
        "account": "student-002",
        "name": "Student 2",
        "site": "HCM",
        "courseCode": "class-001",
        "courseName": _FAMS_COURSE_1_NAME,
        "statusInClass": "InProgress",
    },
    {
        "account": "student-003",
        "name": "Student 3",
        "site": "HN",
        "courseCode": "class-002",
        "courseName": "FAMS Course 2",
        "statusInClass": "InProgress",
    },
]

_GENERATED = generated_vendor_data("fams")
if _GENERATED is not None:
    _CLASSES = _GENERATED["classes"]
    _STUDENTS = _GENERATED["students"]


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
    expected_key = get_mock_settings().mock_fams_token.get_secret_value()
    if api_key != expected_key or accept != "application/json":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid FAMS API key")

    statuses = set(status_filter.split(",")) if status_filter else None
    classes = [
        item
        for item in _CLASSES
        if (statuses is None or item["courseStatus"] in statuses)
        and (site is None or item["site"] == site)
        and (
            actual_start_date_from is None
            or item["actualStartDate"].replace("-", "") >= actual_start_date_from
        )
        and (
            actual_start_date_to is None
            or item["actualStartDate"].replace("-", "") <= actual_start_date_to
        )
    ]
    course_codes = {item["courseCode"] for item in classes}
    students = [item for item in _STUDENTS if item["courseCode"] in course_codes]
    return {
        "success": True,
        "message": "Mock FAMS training data",
        "error_code": "",
        "data": {
            "classList": classes,
            "studentList": students,
        },
    }
