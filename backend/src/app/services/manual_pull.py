from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

from app.models import RunStatus
from app.repositories import CheckpointStore, JobAlreadyRunning

logger = logging.getLogger(__name__)


class IngestionJob(Protocol):
    def __call__(
        self, *, on_started: Callable[[str], None] | None = None
    ) -> Awaitable[object]: ...


class ManualPullManager:
    """Process-local task ownership for manual pulls; vendor services own DB locks."""

    def __init__(self, jobs: Mapping[str, IngestionJob], store: CheckpointStore) -> None:
        self.jobs = jobs
        self.store = store
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    async def start(self, vendor: str) -> str:
        if self._closing:
            raise RuntimeError("Application is shutting down")
        job = self.jobs[vendor]
        started: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        async def execute() -> None:
            try:
                await job(on_started=started.set_result)
                if not started.done():
                    raise RuntimeError("Ingestion finished without creating a run")
            except BaseException:
                if started.done() and not started.cancelled():
                    run_id = started.result()
                    summary = await self.store.get_run(run_id)
                    if summary is not None and summary.status == RunStatus.RUNNING:
                        await self.store.finish_run(
                            run_id, RunStatus.FAILED,
                            "Manual ingestion interrupted; start a new pull to retry",
                        )
                raise

        task = asyncio.create_task(execute(), name=f"manual-pull-{vendor}")
        self._tasks.add(task)
        task.add_done_callback(self._finished)
        # The callback fires only after the service acquired its lock and persisted
        # the run. Schedule/manual races therefore use the same atomic DB lock.
        done, _ = await asyncio.wait(
            (started, task), timeout=30, return_when=asyncio.FIRST_COMPLETED
        )
        if started in done:
            return started.result()
        if task in done:
            task.result()  # Propagate startup failures, including JobAlreadyRunning.
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise TimeoutError("Timed out starting ingestion")

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None and not isinstance(error, JobAlreadyRunning):
                # Startup exceptions may contain source credentials: do not log text.
                logger.error("Manual ingestion task failed error_type=%s", type(error).__name__)

    async def close(self) -> None:
        self._closing = True
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
