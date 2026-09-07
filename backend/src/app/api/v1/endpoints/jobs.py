from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from app.auth.dependencies import AdminDependency
from app.models import RunSummary
from app.repositories import CheckpointStore, JobAlreadyRunning
from app.services.manual_pull import ManualPullManager

VENDOR_KEYS = {
    "levelup": "levelup",
    "skillup": "skillup",
    "datacamp": "datacamp",
    "coursera": "coursera",
    "linkedin": "linkedin",
    "harvard-hmm": "harvard_hmm",
    "harvard-spark": "harvard_spark",
    "fams": "fams",
}


class PullAccepted(BaseModel):
    run_id: str
    vendor: str
    status: Literal["accepted"] = "accepted"
    status_url: str


def build_job_router(
    checkpoints: CheckpointStore,
    require_admin: AdminDependency,
    manual_pulls: ManualPullManager,
) -> APIRouter:
    router = APIRouter(dependencies=[Depends(require_admin)])

    @router.post("/jobs/{vendor}/pull", status_code=status.HTTP_202_ACCEPTED)
    async def pull_vendor(vendor: str, response: Response) -> PullAccepted:
        vendor_key = VENDOR_KEYS.get(vendor)
        if vendor_key is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown vendor")
        if vendor_key not in manual_pulls.jobs:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "Vendor is not configured for ingestion"
            )
        try:
            run_id = await manual_pulls.start(vendor_key)
        except JobAlreadyRunning as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT, "An ingestion for this vendor is already running"
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "Unable to start ingestion"
            ) from exc
        status_url = f"/jobs/runs/{run_id}"
        response.headers["Location"] = status_url
        return PullAccepted(run_id=run_id, vendor=vendor_key, status_url=status_url)

    @router.get("/jobs/runs/{run_id}")
    async def get_job_run(run_id: UUID) -> RunSummary:
        summary = await checkpoints.get_run(str(run_id))
        if summary is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Run not found")
        return summary

    async def latest(vendor: str, display_name: str) -> RunSummary:
        summary = await checkpoints.latest_run(vendor)
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No {display_name} run found",
            )
        return summary

    @router.get("/jobs/levelup/latest")
    async def latest_levelup_job() -> RunSummary:
        return await latest("levelup", "LevelUP")

    @router.get("/jobs/skillup/latest")
    async def latest_skillup_job() -> RunSummary:
        return await latest("skillup", "SkillUp")

    @router.get("/jobs/datacamp/latest")
    async def latest_datacamp_job() -> RunSummary:
        return await latest("datacamp", "DataCamp")

    @router.get("/jobs/coursera/latest")
    async def latest_coursera_job() -> RunSummary:
        return await latest("coursera", "Coursera")

    @router.get("/jobs/linkedin/latest")
    async def latest_linkedin_job() -> RunSummary:
        return await latest("linkedin", "LinkedIn")

    @router.get("/jobs/harvard-hmm/latest")
    async def latest_harvard_hmm_job() -> RunSummary:
        return await latest("harvard_hmm", "Harvard HMM")

    @router.get("/jobs/harvard-spark/latest")
    async def latest_harvard_spark_job() -> RunSummary:
        return await latest("harvard_spark", "Harvard Spark")

    @router.get("/jobs/fams/latest")
    async def latest_fams_job() -> RunSummary:
        return await latest("fams", "FAMS")

    return router
