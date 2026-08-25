from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.repositories import CheckpointStore


def build_health_router(checkpoints: CheckpointStore) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    async def ready() -> dict[str, str]:
        if not await checkpoints.is_ready():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="checkpoint store is not ready",
            )
        return {"status": "ready"}

    return router
