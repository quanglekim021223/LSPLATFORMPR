from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.models import RunSummary
from app.repositories import CheckpointStore


def build_job_router(checkpoints: CheckpointStore) -> APIRouter:
    router = APIRouter()

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
