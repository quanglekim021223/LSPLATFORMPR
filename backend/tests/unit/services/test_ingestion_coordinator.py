from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.models import RunStatus, RunSummary, SafeIngestionError
from app.services.ingestion_coordinator import (
    IngestionAlreadyRunning,
    IngestionCoordinator,
    IngestionState,
    IngestionStatus,
    QueuedIngestion,
)


class FakeDurableBackend:
    def __init__(self) -> None:
        self.states: dict[str, IngestionState] = {}
        self.messages: list[QueuedIngestion] = []

    async def create(self, state: IngestionState) -> None:
        self.states[state.job_id] = state.model_copy(deep=True)

    async def save(self, state: IngestionState) -> None:
        self.states[state.job_id] = state.model_copy(deep=True)

    async def get(self, job_id: str) -> IngestionState | None:
        state = self.states.get(job_id)
        return state.model_copy(deep=True) if state is not None else None

    async def enqueue(self, message: QueuedIngestion) -> None:
        self.messages.append(message.model_copy(deep=True))


def summary(vendor: str, status: RunStatus, records: int = 1) -> RunSummary:
    return RunSummary(
        run_id=f"{vendor}-run",
        vendor=vendor,
        status=status,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        records_by_domain={"course_catalog": records},
        error_message="partial domain failure" if status == RunStatus.PARTIAL_FAILURE else None,
    )


async def wait_for_terminal(coordinator: IngestionCoordinator, job_id: str) -> IngestionStatus:
    for _ in range(100):
        state = await coordinator.get(job_id)
        assert state is not None
        if state.status not in {IngestionStatus.QUEUED, IngestionStatus.RUNNING}:
            return state.status
        await asyncio.sleep(0)
    raise AssertionError("Coordinator did not reach a terminal state")


@pytest.mark.asyncio
async def test_tracks_vendor_run_ids_and_partial_failure() -> None:
    async def levelup() -> RunSummary:
        return summary("levelup", RunStatus.SUCCEEDED, 3)

    async def skillup() -> RunSummary:
        return summary("skillup", RunStatus.PARTIAL_FAILURE, 2)

    coordinator = IngestionCoordinator({"levelup": levelup, "skillup": skillup})
    started = await coordinator.start(["levelup", "skillup"])

    assert await wait_for_terminal(coordinator, started.job_id) == IngestionStatus.PARTIAL_FAILURE
    completed = await coordinator.get(started.job_id)
    assert completed is not None
    assert [item.run_id for item in completed.vendor_runs] == [
        "levelup-run",
        "skillup-run",
    ]
    assert completed.total_records == 5


@pytest.mark.asyncio
async def test_prevents_duplicate_submission_and_records_backend_error() -> None:
    release = asyncio.Event()

    async def blocked() -> RunSummary:
        await release.wait()
        raise RuntimeError("upstream unavailable")

    coordinator = IngestionCoordinator({"levelup": blocked})
    started = await coordinator.start(["levelup"])
    await asyncio.sleep(0)

    with pytest.raises(IngestionAlreadyRunning) as exc_info:
        await coordinator.start(["levelup"])
    assert exc_info.value.job_id == started.job_id

    release.set()
    assert await wait_for_terminal(coordinator, started.job_id) == IngestionStatus.FAILED
    failed = await coordinator.get(started.job_id)
    assert failed is not None
    assert failed.vendor_runs[0].status == IngestionStatus.FAILED
    assert failed.vendor_runs[0].error_message is not None


@pytest.mark.asyncio
async def test_exposes_only_safe_operational_diagnostics() -> None:
    async def safe_failure() -> RunSummary:
        raise SafeIngestionError(
            "stage=vendor_pull vendor=levelup error_type=VendorRunFailed detail=HTTP 401"
        )

    coordinator = IngestionCoordinator({"levelup": safe_failure})
    started = await coordinator.start(["levelup"])

    assert await wait_for_terminal(coordinator, started.job_id) == IngestionStatus.FAILED
    failed = await coordinator.get(started.job_id)
    assert failed is not None
    assert failed.vendor_runs[0].error_message == (
        "stage=vendor_pull vendor=levelup error_type=VendorRunFailed detail=HTTP 401"
    )


@pytest.mark.asyncio
async def test_refreshes_running_vendor_progress_from_persisted_run() -> None:
    release = asyncio.Event()
    progress_time = datetime(2026, 9, 7, 4, 38, 12, tzinfo=UTC)

    async def blocked() -> RunSummary:
        await release.wait()
        return summary("levelup", RunStatus.SUCCEEDED, 1_000)

    async def progress_reader(vendor: str) -> RunSummary:
        return RunSummary(
            run_id=f"{vendor}-active-run",
            vendor=vendor,
            status=RunStatus.RUNNING,
            started_at=datetime.now(UTC),
            last_progress_at=progress_time,
            records_by_domain={"course_catalog": 200},
        )

    coordinator = IngestionCoordinator(
        {"levelup": blocked},
        progress_reader=progress_reader,
    )
    started = await coordinator.start(["levelup"])
    for _ in range(10):
        current = await coordinator.get(started.job_id)
        assert current is not None
        if current.vendor_runs[0].status == IngestionStatus.RUNNING:
            break
        await asyncio.sleep(0)

    running = await coordinator.get(started.job_id)

    assert running is not None
    assert running.vendor_runs[0].run_id == "levelup-active-run"
    assert running.vendor_runs[0].records_by_domain == {"course_catalog": 200}
    assert running.vendor_runs[0].total_records == 200
    assert running.vendor_runs[0].last_progress_at == progress_time

    release.set()
    assert await wait_for_terminal(coordinator, started.job_id) == IngestionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_durable_backend_tracks_job_across_coordinator_instances() -> None:
    backend = FakeDurableBackend()

    async def levelup() -> RunSummary:
        return summary("levelup", RunStatus.SUCCEEDED, 3)

    submitter = IngestionCoordinator({"levelup": levelup}, backend=backend)
    worker = IngestionCoordinator({"levelup": levelup}, backend=backend)

    started = await submitter.start(["levelup"])
    assert started.status == IngestionStatus.QUEUED
    assert len(backend.messages) == 1

    await worker.run_queued(backend.messages[0])
    completed = await submitter.get(started.job_id)

    assert completed is not None
    assert completed.status == IngestionStatus.SUCCEEDED
    assert completed.vendor_runs[0].run_id == "levelup-run"
    assert completed.total_records == 3
