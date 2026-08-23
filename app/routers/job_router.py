from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.models import RunSummary
from app.repositories.checkpoint_repository import CheckpointStore


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

    return router
