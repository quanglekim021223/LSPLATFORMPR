from __future__ import annotations
 
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
 
from app.auth.dependencies import AdminDependency
from app.services.ingestion_coordinator import (
    IngestionAlreadyRunning,
    IngestionCoordinator,
    IngestionState,
    VendorsNotConfigured,
)
 
 
class StartIngestionRequest(BaseModel):
    vendors: list[str] = Field(min_length=1)
 
 
def build_ingestion_router(
    coordinator: IngestionCoordinator,
    require_admin: AdminDependency,
) -> APIRouter:
    router = APIRouter(
        prefix="/ingestions",
        tags=["ingestions"],
        dependencies=[Depends(require_admin)],
    )
 
    @router.post("", response_model=IngestionState, status_code=status.HTTP_202_ACCEPTED)
    async def start_ingestion(request: StartIngestionRequest) -> IngestionState:
        try:
            return await coordinator.start(request.vendors)
        except IngestionAlreadyRunning as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": str(exc), "job_id": exc.job_id},
            ) from exc
        except VendorsNotConfigured as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": str(exc), "vendors": exc.vendors},
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
 
    @router.get("/{job_id}", response_model=IngestionState)
    async def ingestion_status(job_id: str) -> IngestionState:
        state = await coordinator.get(job_id)
        if state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Ingestion job not found",
            )
        return state
 
    return router
 