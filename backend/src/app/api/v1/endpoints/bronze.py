from __future__ import annotations
 
from datetime import UTC, datetime
from typing import Annotated
 
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
 
from app.auth.dependencies import AdminDependency
from app.repositories import CheckpointStore, JobAlreadyRunning, LocalBronzeWriter
from app.repositories.writer import BronzeWriter
from app.services.ingestion_coordinator import IngestionCoordinator
 
SUPPORTED_VENDORS = {
    "levelup",
    "skillup",
    "datacamp",
    "coursera",
    "linkedin",
    "harvard_hmm",
    "harvard_spark",
    "fams",
}
 
 
class CleanupRequest(BaseModel):
    vendors: list[str] = Field(min_length=1)
 
 
class CleanupResponse(BaseModel):
    vendors: list[str]
    runs_deleted: int
    objects_deleted: int
    bytes_deleted: int
 
 
def _validated_vendors(vendors: list[str]) -> list[str]:
    unique = list(dict.fromkeys(vendors))
    invalid = [vendor for vendor in unique if vendor not in SUPPORTED_VENDORS]
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Unsupported vendors", "vendors": invalid},
        )
    return unique
 
 
def _export_filename(vendors: list[str], date: str) -> str:
    if len(vendors) == 1:
        vendor_name = vendors[0].replace("_", "-")
        return f"bronze-{vendor_name}-{date}.csv"
    return f"bronze-export-{date}.csv"
 
 
def build_bronze_router(
    checkpoints: CheckpointStore,
    bronze_writer: BronzeWriter,
    coordinator: IngestionCoordinator,
    require_admin: AdminDependency,
) -> APIRouter:
    router = APIRouter(
        prefix="/bronze",
        tags=["bronze"],
        dependencies=[Depends(require_admin)],
    )
 
    @router.get("/export.csv")
    async def export_csv(
        vendor: Annotated[list[str] | None, Query()] = None,
    ) -> Response:
        vendors = _validated_vendors(vendor or sorted(SUPPORTED_VENDORS))
        if not isinstance(bronze_writer, LocalBronzeWriter):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="CSV export currently supports local Bronze storage only",
            )
        csv_payload = await bronze_writer.export_csv(vendors)
        date = datetime.now(UTC).date().isoformat()
        filename = _export_filename(vendors, date)
        return Response(
            content=csv_payload,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )
 
    @router.delete("")
    async def cleanup(request: CleanupRequest) -> CleanupResponse:
        vendors = _validated_vendors(request.vendors)
        if coordinator.has_active_vendor(vendors):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot clear Bronze data while ingestion is running",
            )
        if not isinstance(bronze_writer, LocalBronzeWriter):
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Cleanup currently supports local Bronze storage only",
            )
        locked_vendor = await checkpoints.locked_vendor(vendors)
        if locked_vendor is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Cannot clear Bronze data while {locked_vendor} is running",
            )
        try:
            objects_deleted, bytes_deleted = await bronze_writer.clear_vendors(vendors)
            runs_deleted = await checkpoints.delete_vendor_data(vendors)
        except JobAlreadyRunning as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=str(exc)
            ) from exc
        return CleanupResponse(
            vendors=vendors,
            runs_deleted=runs_deleted,
            objects_deleted=objects_deleted,
            bytes_deleted=bytes_deleted,
        )
 
    return router
