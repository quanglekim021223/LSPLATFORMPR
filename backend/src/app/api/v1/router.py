from __future__ import annotations

from fastapi import APIRouter

from app.api.v1.endpoints.auth import build_auth_router
from app.api.v1.endpoints.health import build_health_router
from app.api.v1.endpoints.jobs import build_job_router
from app.auth.dependencies import build_admin_dependency
from app.core.config import Settings
from app.repositories import CheckpointStore


def build_api_router(checkpoints: CheckpointStore, settings: Settings) -> APIRouter:
    router = APIRouter()
    router.include_router(build_health_router(checkpoints))
    router.include_router(build_auth_router(settings))
    router.include_router(
        build_job_router(checkpoints, build_admin_dependency(settings))
    )
    return router
