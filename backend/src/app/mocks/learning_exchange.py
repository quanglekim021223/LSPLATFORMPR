"""Standalone local FHU/akajob/SkillUp simulator, isolated from existing mocks.

Only the two POST wire contracts are taken from the supplied screenshots.
Source APIs, exact title/issuer matching, deduplication and delayed publication
are demo assumptions. This module is not imported by the production app.
"""

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from app.mocks.learning_exchange_data import (
    DEMO_API_KEY,
    DEMO_MARKER,
    DEMO_SKILLS,
    SOURCE_FILE,
    DemoSources,
)
from app.schemas.skillup.certificates import CreateCertificate, EmployeeCertificate, SkillMapping


def _authorize(x_api_key: Annotated[str | None, Header()] = None) -> None:
    if x_api_key != DEMO_API_KEY:
        raise HTTPException(401, "Invalid demo API key")


def _profile_skill(skill: SkillMapping) -> dict[str, Any]:
    # Placeholder scores required by the existing read contract, NOT real assessment.
    return {
        "skill": {
            "skillId": skill["skillId"],
            "skillName": skill["skillName"],
            "taxonomySkillExternalId": None,
            "modifiedOn": datetime.now(UTC).isoformat(),
            "skillClassification": {"classificationId": 504, "classificationName": "Demo"},
        },
        "selfValidationScore": 0,
        "iMochaValidationScore": None,
        "managerValidationScore": None,
        "weightedProficiencyScore": 0,
        "multiRaterValidationScore": None,
        "weightedAIInferenceScore": None,
        "experienceInMonths": None,
        "aiInferredRatings": [],
        "isJobProfileSkill": False,
        "skillPriorirty": None,
        "skillRequiredProficiency": 0,
        "skillGapInPercentage": 0,
    }


def create_demo_app(source_file: Path = SOURCE_FILE, *, processing_delay: float = 0.2) -> FastAPI:
    def sources() -> DemoSources:
        # Read on every source request so editing JSON is visible without restart.
        return DemoSources.model_validate_json(source_file.read_bytes())

    # Demo prerequisite: these employees already exist in SkillUp at startup.
    registered_employees = sources().employees
    catalog: dict[tuple[str, str], CreateCertificate] = {}
    uploads: dict[tuple[str, str, str], tuple[EmployeeCertificate, float]] = {}
    known_skills = {item["taxonomySkillId"]: item for item in DEMO_SKILLS.values()}
    app = FastAPI(title="LOCAL ONLY: Learning Exchange Demo")
    app.state.demo_catalog = catalog
    app.state.demo_uploads = uploads

    @app.get("/demo/info")
    async def info() -> dict[str, str]:
        return {"service": DEMO_MARKER}

    @app.get("/fhu/employees", dependencies=[Depends(_authorize)])
    def employees() -> dict[str, Any]:
        return {"items": [item.model_dump(by_alias=True) for item in sources().employees]}

    @app.get("/akajob/certificates", dependencies=[Depends(_authorize)])
    def certificates() -> dict[str, Any]:
        return {
            "items": [
                item.model_dump(by_alias=True, exclude_none=True) for item in sources().certificates
            ]
        }

    @app.post("/skillup/certificates/create", dependencies=[Depends(_authorize)])
    async def create_certificate(body: CreateCertificate) -> CreateCertificate:
        if any(item.taxonomy_skill_id not in known_skills for item in body.skills):
            raise HTTPException(422, "Unknown demo taxonomy skill")
        catalog[(body.title, body.certificate_issuer)] = body
        return body

    @app.post(
        "/skillup/employees/{employee_id}/certificates",
        status_code=201,
        dependencies=[Depends(_authorize)],
    )
    async def upload_certificate(
        employee_id: str, body: list[EmployeeCertificate]
    ) -> dict[str, Any]:
        if not any(item.external_employee_id == employee_id for item in registered_employees):
            raise HTTPException(404, "Employee must already exist in mock SkillUp")
        # Validate the entire batch before mutating the mock state.
        for item in body:
            if not item.is_standard_certificate:
                raise HTTPException(422, "Demo supports standard certificates only")
            if (item.title, item.issuer or "") not in catalog:
                raise HTTPException(422, "Certificate title/issuer not found in demo catalog")
        for item in body:
            key = (employee_id, item.title, item.issuer or "")
            previous = uploads.get(key)
            if previous is None or previous[0] != item:
                uploads[key] = (item, time.monotonic() + processing_delay)
        return {}

    @app.get("/skillup/employees/skills-profile", dependencies=[Depends(_authorize)])
    async def skill_profiles(
        page_number: Annotated[int, Query(alias="pageNumber", ge=1)] = 1,
        page_size: Annotated[int, Query(alias="pageSize", ge=1, le=100)] = 100,
    ) -> dict[str, Any]:
        profiles = []
        for index, employee in enumerate(registered_employees):
            taxonomy_ids: set[int] = set()
            for (external_id, title, issuer), (_, ready_at) in uploads.items():
                if external_id == employee.external_employee_id and time.monotonic() >= ready_at:
                    taxonomy_ids.update(
                        item.taxonomy_skill_id for item in catalog[(title, issuer)].skills
                    )
            profiles.append(
                {
                    "employeeId": index + 1,
                    **employee.model_dump(by_alias=True),
                    "skills": [_profile_skill(known_skills[key]) for key in sorted(taxonomy_ids)],
                }
            )
        total_pages = (len(profiles) + page_size - 1) // page_size
        start = (page_number - 1) * page_size
        return {
            "items": profiles[start : start + page_size],
            "pageNumber": page_number,
            "totalPages": total_pages,
            "totalCount": len(profiles),
            "hasPreviousPage": page_number > 1,
            "hasNextPage": page_number < total_pages,
        }

    return app


app = create_demo_app(
    Path(os.environ.get("LEARNING_EXCHANGE_DEMO_SOURCE_FILE", str(SOURCE_FILE))),
    processing_delay=float(os.environ.get("LEARNING_EXCHANGE_DEMO_DELAY_SECONDS", "0.2")),
)
