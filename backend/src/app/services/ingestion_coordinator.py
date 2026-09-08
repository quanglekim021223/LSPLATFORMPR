from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from app.models import RunSummary

logger = logging.getLogger(__name__)
IngestionJob = Callable[[], Awaitable[object]]
ProgressReader = Callable[[str], Awaitable[RunSummary | None]]


class IngestionStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {
    IngestionStatus.SUCCEEDED,
    IngestionStatus.PARTIAL_FAILURE,
    IngestionStatus.FAILED,
    IngestionStatus.CANCELLED,
}


class VendorIngestionState(BaseModel):
    vendor: str
    run_id: str | None = None
    status: IngestionStatus = IngestionStatus.QUEUED
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_progress_at: datetime | None = None
    records_by_domain: dict[str, int] = Field(default_factory=dict)
    total_records: int = 0
    error_message: str | None = None


class IngestionState(BaseModel):
    job_id: str
    status: IngestionStatus = IngestionStatus.QUEUED
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    vendor_runs: list[VendorIngestionState]
    total_records: int = 0
    error_message: str | None = None


class IngestionAlreadyRunning(RuntimeError):
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Ingestion job {job_id} is already running")


class VendorsNotConfigured(ValueError):
    def __init__(self, vendors: Sequence[str]) -> None:
        self.vendors = list(vendors)
        super().__init__(f"Vendors are not configured: {', '.join(self.vendors)}")


class IngestionCoordinator:
    def __init__(
        self,
        jobs: dict[str, IngestionJob],
        *,
        progress_reader: ProgressReader | None = None,
    ) -> None:
        self._jobs = jobs
        self._progress_reader = progress_reader
        self._states: dict[str, IngestionState] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._active_job_id: str | None = None
        self._lock = asyncio.Lock()

    @property
    def configured_vendors(self) -> set[str]:
        return set(self._jobs)

    async def start(self, vendors: Sequence[str]) -> IngestionState:
        unique_vendors = list(dict.fromkeys(vendors))
        missing = [vendor for vendor in unique_vendors if vendor not in self._jobs]
        if not unique_vendors:
            raise ValueError("At least one vendor is required")
        if missing:
            raise VendorsNotConfigured(missing)

        async with self._lock:
            if self._active_job_id is not None:
                active = self._states[self._active_job_id]
                if active.status not in TERMINAL_STATUSES:
                    raise IngestionAlreadyRunning(active.job_id)

            job_id = str(uuid4())
            state = IngestionState(
                job_id=job_id,
                created_at=datetime.now(UTC),
                vendor_runs=[VendorIngestionState(vendor=vendor) for vendor in unique_vendors],
            )
            self._states[job_id] = state
            self._active_job_id = job_id
            self._tasks[job_id] = asyncio.create_task(
                self._run(state), name=f"manual-ingestion-{job_id}"
            )
            return state.model_copy(deep=True)

    async def get(self, job_id: str) -> IngestionState | None:
        state = self._states.get(job_id)
        if state is not None:
            await self._refresh_progress(state)
        return state.model_copy(deep=True) if state is not None else None

    async def _refresh_progress(self, state: IngestionState) -> None:
        if self._progress_reader is None or state.started_at is None:
            return
        for vendor_state in state.vendor_runs:
            if vendor_state.status != IngestionStatus.RUNNING:
                continue
            summary = await self._progress_reader(vendor_state.vendor)
            if (
                vendor_state.status != IngestionStatus.RUNNING
                or summary is None
                or summary.started_at < state.started_at
            ):
                continue
            vendor_state.run_id = summary.run_id
            vendor_state.started_at = summary.started_at
            vendor_state.finished_at = summary.finished_at
            vendor_state.last_progress_at = summary.last_progress_at
            vendor_state.records_by_domain = summary.records_by_domain
            vendor_state.total_records = sum(summary.records_by_domain.values())

    def has_active_vendor(self, vendors: Sequence[str]) -> bool:
        if self._active_job_id is None:
            return False
        active = self._states[self._active_job_id]
        requested = set(vendors)
        return active.status not in TERMINAL_STATUSES and any(
            item.vendor in requested for item in active.vendor_runs
        )

    async def shutdown(self) -> None:
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, state: IngestionState) -> None:
        state.status = IngestionStatus.RUNNING
        state.started_at = datetime.now(UTC)
        try:
            await asyncio.gather(
                *(self._run_vendor(vendor) for vendor in state.vendor_runs)
            )
            state.total_records = sum(vendor.total_records for vendor in state.vendor_runs)
            statuses = {vendor.status for vendor in state.vendor_runs}
            if statuses == {IngestionStatus.SUCCEEDED}:
                state.status = IngestionStatus.SUCCEEDED
            elif statuses <= {IngestionStatus.FAILED, IngestionStatus.CANCELLED}:
                state.status = (
                    IngestionStatus.CANCELLED
                    if statuses == {IngestionStatus.CANCELLED}
                    else IngestionStatus.FAILED
                )
            else:
                state.status = IngestionStatus.PARTIAL_FAILURE
        except asyncio.CancelledError:
            state.status = IngestionStatus.CANCELLED
            for vendor in state.vendor_runs:
                if vendor.status not in TERMINAL_STATUSES:
                    vendor.status = IngestionStatus.CANCELLED
            raise
        finally:
            state.finished_at = datetime.now(UTC)
            if self._active_job_id == state.job_id:
                self._active_job_id = None

    async def _run_vendor(
        self, vendor_state: VendorIngestionState
    ) -> None:
        vendor_state.status = IngestionStatus.RUNNING
        vendor_state.started_at = datetime.now(UTC)
        try:
            result = await self._jobs[vendor_state.vendor]()
            if not isinstance(result, RunSummary):
                raise TypeError("Vendor ingestion did not return a run summary")
            vendor_state.run_id = result.run_id
            vendor_state.started_at = result.started_at
            vendor_state.finished_at = result.finished_at
            vendor_state.last_progress_at = result.last_progress_at
            vendor_state.records_by_domain = result.records_by_domain
            vendor_state.total_records = sum(result.records_by_domain.values())
            vendor_state.error_message = result.error_message
            vendor_state.status = IngestionStatus(result.status.value)
        except asyncio.CancelledError:
            vendor_state.status = IngestionStatus.CANCELLED
            raise
        except Exception:
            logger.exception("Manual ingestion failed vendor=%s", vendor_state.vendor)
            vendor_state.status = IngestionStatus.FAILED
            vendor_state.error_message = "Vendor ingestion failed before producing a run summary"
        finally:
            if vendor_state.finished_at is None:
                vendor_state.finished_at = datetime.now(UTC)
