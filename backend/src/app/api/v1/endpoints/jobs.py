from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.models import RunSummary
from app.repositories import CheckpointStore


def build_job_router(checkpoints: CheckpointStore) -> APIRouter:
    router = APIRouter()

    @router.get("/jobs/levelup/latest", response_model=RunSummary)
    async def latest_levelup_job() -> RunSummary:
        summary = await checkpoints.latest_run("levelup")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No LevelUP run found",
            )
        return summary

    @router.get("/jobs/skillup/latest", response_model=RunSummary)
    async def latest_skillup_job() -> RunSummary:
        summary = await checkpoints.latest_run("skillup")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No SkillUp run found",
            )
        return summary

    @router.get("/jobs/datacamp/latest", response_model=RunSummary)
    async def latest_datacamp_job() -> RunSummary:
        summary = await checkpoints.latest_run("datacamp")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No DataCamp run found",
            )
        return summary

    @router.get("/jobs/coursera/latest", response_model=RunSummary)
    async def latest_coursera_job() -> RunSummary:
        summary = await checkpoints.latest_run("coursera")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Coursera run found",
            )
        return summary

    @router.get("/jobs/linkedin/latest", response_model=RunSummary)
    async def latest_linkedin_job() -> RunSummary:
        summary = await checkpoints.latest_run("linkedin")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No LinkedIn run found",
            )
        return summary

    @router.get("/jobs/harvard-hmm/latest", response_model=RunSummary)
    async def latest_harvard_hmm_job() -> RunSummary:
        summary = await checkpoints.latest_run("harvard_hmm")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Harvard HMM run found",
            )
        return summary

    @router.get("/jobs/harvard-spark/latest", response_model=RunSummary)
    async def latest_harvard_spark_job() -> RunSummary:
        summary = await checkpoints.latest_run("harvard_spark")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Harvard Spark run found",
            )
        return summary

    @router.get("/jobs/fams/latest", response_model=RunSummary)
    async def latest_fams_job() -> RunSummary:
        summary = await checkpoints.latest_run("fams")
        if summary is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No FAMS run found",
            )
        return summary

    return router
