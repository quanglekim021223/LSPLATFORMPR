from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from pydantic import BaseModel, Field

from app.models import RunSummary, SafeIngestionError

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
    restored_records: int = 0
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


class QueuedIngestion(BaseModel):
    job_id: str
    vendors: list[str]


class IngestionBackend(Protocol):
    async def create(self, state: IngestionState) -> None: ...

    async def save(self, state: IngestionState) -> None: ...

    async def get(self, job_id: str) -> IngestionState | None: ...

    async def enqueue(self, message: QueuedIngestion) -> None: ...


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
        backend: IngestionBackend | None = None,
    ) -> None:
        self._jobs = jobs
        self._progress_reader = progress_reader
        self._backend = backend
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
            if self._backend is not None:
                await self._backend.create(state)
                try:
                    await self._backend.enqueue(
                        QueuedIngestion(job_id=job_id, vendors=unique_vendors)
                    )
                except Exception:
                    state.status = IngestionStatus.FAILED
                    state.finished_at = datetime.now(UTC)
                    state.error_message = "Failed to queue ingestion job"
                    await self._backend.save(state)
                    raise
                return state.model_copy(deep=True)
            self._states[job_id] = state
            self._active_job_id = job_id
            self._tasks[job_id] = asyncio.create_task(
                self._run(state), name=f"manual-ingestion-{job_id}"
            )
            return state.model_copy(deep=True)

    async def get(self, job_id: str) -> IngestionState | None:
        state = (
            await self._backend.get(job_id)
            if self._backend is not None
            else self._states.get(job_id)
        )
        if state is not None:
            await self._refresh_progress(state)
        return state.model_copy(deep=True) if state is not None else None

    async def run_queued(self, message: QueuedIngestion) -> IngestionState:
        if self._backend is None:
            raise RuntimeError("Durable ingestion backend is not configured")
        state = await self._backend.get(message.job_id)
        if state is None:
            raise ValueError(f"Ingestion job {message.job_id} is missing durable state")
        if [item.vendor for item in state.vendor_runs] != message.vendors:
            raise ValueError(f"Ingestion job {message.job_id} vendor mismatch")
        if state.status == IngestionStatus.SUCCEEDED:
            return state
        await self._run(state)
        completed = await self._backend.get(message.job_id)
        if completed is None or completed.status != IngestionStatus.SUCCEEDED:
            raise RuntimeError(f"Ingestion job {message.job_id} did not succeed")
        return completed

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
            vendor_state.restored_records = summary.restored_records

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
        logger.info(
            "Manual ingestion started job_id=%s vendors=%s",
            state.job_id,
            ",".join(vendor.vendor for vendor in state.vendor_runs),
        )
        state.status = IngestionStatus.RUNNING
        state.started_at = datetime.now(UTC)
        state.finished_at = None
        state.error_message = None
        for vendor in state.vendor_runs:
            vendor.status = IngestionStatus.RUNNING
            vendor.started_at = state.started_at
            vendor.finished_at = None
            vendor.error_message = None
        await self._persist(state)
        try:
            await asyncio.gather(*(self._run_vendor(vendor) for vendor in state.vendor_runs))
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
            await self._persist(state)
            logger.info(
                "Manual ingestion finished job_id=%s status=%s records=%d",
                state.job_id,
                state.status.value,
                state.total_records,
            )
            if self._active_job_id == state.job_id:
                self._active_job_id = None

    async def _persist(self, state: IngestionState) -> None:
        if self._backend is not None:
            await self._backend.save(state)

    async def _run_vendor(self, vendor_state: VendorIngestionState) -> None:
        logger.info("Manual vendor ingestion started vendor=%s", vendor_state.vendor)
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
            vendor_state.restored_records = result.restored_records
            vendor_state.error_message = result.error_message
            vendor_state.status = IngestionStatus(result.status.value)
        except asyncio.CancelledError:
            vendor_state.status = IngestionStatus.CANCELLED
            raise
        except Exception as exc:
            vendor_state.status = IngestionStatus.FAILED
            vendor_state.error_message = (
                str(exc)
                if isinstance(exc, SafeIngestionError)
                else "Vendor ingestion failed before producing a run summary"
            )
            logger.error(
                "Manual vendor ingestion failed vendor=%s error_type=%s diagnostic=%s",
                vendor_state.vendor,
                type(exc).__name__,
                vendor_state.error_message,
            )
        finally:
            if vendor_state.finished_at is None:
                vendor_state.finished_at = datetime.now(UTC)
            logger.info(
                "Manual vendor ingestion finished vendor=%s status=%s run_id=%s records=%d",
                vendor_state.vendor,
                vendor_state.status.value,
                vendor_state.run_id or "none",
                vendor_state.total_records,
            )
