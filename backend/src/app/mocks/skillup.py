from __future__ import annotations

from copy import deepcopy
from math import ceil
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

from app.mocks.generated_data import generated_vendor_data
from app.mocks.settings import get_mock_settings

router = APIRouter(tags=["SkillUp"])
INFORMATION_TECHNOLOGY = "Information Technology"
DATA_ENGINEERING = "Data Engineering"
_TAXONOMY: list[dict[str, Any]] = [
    {
        "taxonomySkillId": 97915,
        "externalId": None,
        "domain": {"id": 429, "name": INFORMATION_TECHNOLOGY},
        "subdomain": {"id": 2155, "name": INFORMATION_TECHNOLOGY},
        "skillCluster": {"id": 17191, "name": "Programming"},
        "skillClassification": {
            "classificationId": 507,
            "classificationName": "Tool",
        },
        "skill": {
            "id": 93285,
            "name": "Python",
            "description": "Python programming language",
        },
        "displayName": "Python",
        "description": "Python programming language",
        "isCritical": True,
        "taxonomySkillTags": [],
        "skillRubrics": None,
    },
    {
        "taxonomySkillId": 97916,
        "externalId": None,
        "domain": {"id": 429, "name": INFORMATION_TECHNOLOGY},
        "subdomain": {"id": 2155, "name": INFORMATION_TECHNOLOGY},
        "skillCluster": {"id": 17192, "name": "Databases"},
        "skillClassification": {
            "classificationId": 507,
            "classificationName": "Tool",
        },
        "skill": {"id": 93286, "name": "SQL", "description": "SQL language"},
        "displayName": "SQL",
        "description": "SQL language",
        "isCritical": True,
        "taxonomySkillTags": [],
        "skillRubrics": None,
    },
    {
        "taxonomySkillId": 97917,
        "externalId": None,
        "domain": {"id": 429, "name": INFORMATION_TECHNOLOGY},
        "subdomain": {"id": 2155, "name": INFORMATION_TECHNOLOGY},
        "skillCluster": {"id": 17193, "name": DATA_ENGINEERING},
        "skillClassification": {
            "classificationId": 504,
            "classificationName": "Technical Skill",
        },
        "skill": {
            "id": 93287,
            "name": DATA_ENGINEERING,
            "description": "Data engineering practices",
        },
        "displayName": DATA_ENGINEERING,
        "description": "Data engineering practices",
        "isCritical": True,
        "taxonomySkillTags": [],
        "skillRubrics": None,
    },
]
_SKILL_PROFILES: list[dict[str, Any]] = [
    {
        "employeeId": 123456,
        "externalEmployeeId": "00123456",
        "email": "an.nguyen@example.test",
        "fullName": "An Nguyen",
        "skills": [],
    },
    {
        "employeeId": 123457,
        "externalEmployeeId": "00123457",
        "email": "binh.tran@example.test",
        "fullName": "Binh Tran",
        "skills": [
            {
                "skill": {
                    "skillId": 100628,
                    "skillName": "AI Agent",
                    "taxonomySkillExternalId": None,
                    "modifiedOn": "2026-07-23T06:23:23.623",
                    "skillClassification": {
                        "classificationId": 504,
                        "classificationName": "Technical Skill",
                    },
                },
                "selfValidationScore": 3,
                "iMochaValidationScore": None,
                "managerValidationScore": None,
                "weightedProficiencyScore": 3.0,
                "multiRaterValidationScore": None,
                "weightedAIInferenceScore": None,
                "experienceInMonths": None,
                "aiInferredRatings": [],
                "isJobProfileSkill": False,
                "skillPriorirty": None,
                "skillRequiredProficiency": 4,
                "skillGapInPercentage": 25,
            }
        ],
    },
    {
        "employeeId": 123458,
        "externalEmployeeId": "00123458",
        "email": "chi.le@example.test",
        "fullName": "Chi Le",
        "skills": [],
    },
]
_REPORTS: list[dict[str, Any]] = [
    {
        "candidateFullName": "An Nguyen",
        "status": "Complete",
        "appearedOn": "2026-08-20T08:00:47.39",
        "testInvitationId": 13898820,
        "testId": 1346106,
        "candidateEmail": "an.nguyen@example.test",
        "completedOn": "2026-08-20T08:10:38.083",
        "score": 20.0,
        "candidatePoints": 20.0,
        "totalTestPoints": 20.0,
        "scorePercentage": 100.0,
        "timeTaken": 567,
        "testDuration": 30,
        "performanceCategory": "Passed",
        "testName": "Soft Skill Assessment",
        "pdfReportUrl": "https://example.test/reports/13898820.pdf",
        "sections": [
            {
                "sectionID": 2259754,
                "sectionName": "Design thinking",
                "noOfQue": 5,
                "sectionTime": 7,
                "sectionTimeTaken": 156,
                "candidateScore": 5.0,
                "sectionScore": 5.0,
                "negativeMark": 0.0,
                "correctQuestions": 5,
                "wrongQuestions": 0,
                "skippedQuestions": 0,
                "notAnsweredQuestions": 0,
            }
        ],
    },
    {
        "candidateFullName": "Binh Tran",
        "status": "Complete",
        "appearedOn": "2026-08-20T08:00:41.43",
        "testInvitationId": 13898826,
        "testId": 1346106,
        "candidateEmail": "binh.tran@example.test",
        "completedOn": "2026-08-20T08:12:05.453",
        "score": 19.0,
        "candidatePoints": 19.0,
        "totalTestPoints": 20.0,
        "scorePercentage": 95.0,
        "timeTaken": 656,
        "testDuration": 30,
        "performanceCategory": "Passed",
        "testName": "Soft Skill Assessment",
        "pdfReportUrl": "https://example.test/reports/13898826.pdf",
        "sections": [],
    },
    {
        "candidateFullName": "Chi Le",
        "status": "Complete",
        "appearedOn": "2026-08-20T07:59:49.393",
        "testInvitationId": 13898809,
        "testId": 1318369,
        "candidateEmail": "chi.le@example.test",
        "completedOn": "2026-08-20T09:19:52.82",
        "score": 27.63,
        "candidatePoints": 27.63,
        "totalTestPoints": 40.0,
        "scorePercentage": 69.0,
        "timeTaken": 4765,
        "testDuration": 120,
        "performanceCategory": "Experienced",
        "testName": "Engineering Core Assessment",
        "pdfReportUrl": "https://example.test/reports/13898809.pdf",
        "sections": [],
    },
]

_TAXONOMY_MODIFIED_ON = {
    97915: "2026-08-20T01:00:00Z",
    97916: "2026-08-20T02:00:00Z",
    97917: "2026-08-20T03:00:00Z",
}
_SKILL_PROFILE_MODIFIED_ON = {
    123456: "2026-08-20T01:00:00Z",
    123457: "2026-08-20T02:00:00Z",
    123458: "2026-08-20T03:00:00Z",
}

_GENERATED = generated_vendor_data("skillup")
if _GENERATED is not None:
    _TAXONOMY = _GENERATED["taxonomy"]
    _SKILL_PROFILES = _GENERATED["skill_profiles"]
    _REPORTS = _GENERATED["reports"]
    _TAXONOMY_MODIFIED_ON = {
        int(key): value
        for key, value in _GENERATED["taxonomy_modified_on"].items()
    }
    _SKILL_PROFILE_MODIFIED_ON = {
        int(key): value
        for key, value in _GENERATED["skill_profile_modified_on"].items()
    }


def taxonomy_item(index: int = 0) -> dict[str, Any]:
    return deepcopy(_TAXONOMY[index])


def skill_profile(index: int = 0) -> dict[str, Any]:
    return deepcopy(_SKILL_PROFILES[index])


def assessment_report(index: int = 0) -> dict[str, Any]:
    return deepcopy(_REPORTS[index])


def _validate_api_key(api_key: str | None) -> None:
    if api_key != get_mock_settings().mock_skillup_api_key.get_secret_value():
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
    last_modified_on: Annotated[str | None, Query(alias="LastModifiedOn")] = None,
    api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> dict[str, Any]:
    _validate_api_key(api_key)
    records = _TAXONOMY
    if last_modified_on:
        records = [
            record
            for record in records
            if _TAXONOMY_MODIFIED_ON[int(record["taxonomySkillId"])]
            > last_modified_on
        ]
    items, total_pages = _page(records, page_number, page_size)
    return {
        "items": items,
        "pageNumber": page_number,
        "totalPages": total_pages,
        "totalCount": len(records),
        "hasPreviousPage": page_number > 1,
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
    _validate_api_key(api_key)
    records = _SKILL_PROFILES
    if skill_profile_modified_since:
        records = [
            record
            for record in records
            if _SKILL_PROFILE_MODIFIED_ON[int(record["employeeId"])]
            > skill_profile_modified_since
        ]
    if search_text:
        query = search_text.casefold()
        records = [
            record
            for record in records
            if query in str(record["fullName"]).casefold()
        ]
    items, total_pages = _page(records, page_number, page_size)
    return {
        "items": items,
        "pageNumber": page_number,
        "totalPages": total_pages,
        "totalCount": len(records),
        "hasPreviousPage": page_number > 1,
        "hasNextPage": page_number < total_pages,
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
    if not include_sections:
        reports = [
            {key: value for key, value in report.items() if key != "sections"}
            for report in reports
        ]
    return {
        "reports": reports,
        "pageNumber": page_number,
        "totalPages": total_pages,
        "totalCount": len(_REPORTS),
        "hasPreviousPage": page_number > 1,
        "hasNextPage": page_number < total_pages,
    }
