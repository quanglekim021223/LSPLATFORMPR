from __future__ import annotations
 
from fastapi import APIRouter
 
from app.api.v1.endpoints.auth import build_auth_router
from app.api.v1.endpoints.bronze import build_bronze_router
from app.api.v1.endpoints.health import build_health_router
from app.api.v1.endpoints.ingestions import build_ingestion_router
from app.api.v1.endpoints.jobs import build_job_router
from app.auth.dependencies import build_admin_dependency
from app.core.config import Settings
from app.repositories import BronzeWriter, CheckpointStore
from app.services.ingestion_coordinator import IngestionCoordinator
 
 
def build_api_router(
    checkpoints: CheckpointStore,
    settings: Settings,
    bronze_writer: BronzeWriter,
    coordinator: IngestionCoordinator,
) -> APIRouter:
    router = APIRouter()
    require_admin = build_admin_dependency(settings)
    router.include_router(build_health_router(checkpoints))
    router.include_router(build_auth_router(settings))
    router.include_router(build_job_router(checkpoints, require_admin))
    router.include_router(build_ingestion_router(coordinator, require_admin))
    router.include_router(
        build_bronze_router(checkpoints, bronze_writer, coordinator, require_admin)
    )
    return router