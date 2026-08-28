from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from app.clients.levelup_client import LevelUpClient
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import RunStatus, RunSummary
from app.repositories import BronzeWriter, CheckpointStore, LocalBronzeWriter
from app.services.levelup.course_catalog import DOMAIN as COURSE_CATALOG
from app.services.levelup.course_catalog import ingest_course_catalog
from app.services.levelup.learning_history import ingest_learning_history

logger = logging.getLogger(__name__)


class LevelUpJob:
    def __init__(
        self,
        settings: Settings,
        client: LevelUpClient,
        checkpoint_store: CheckpointStore,
        bronze_writer: BronzeWriter,
    ) -> None:
        self.settings = settings
        self.client = client
        self.checkpoints = checkpoint_store
        self.writer = bronze_writer
        self._heartbeat_error: Exception | None = None

    async def run(self) -> RunSummary:
        self._heartbeat_error = None
        current_run_id = str(uuid4())

        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("LevelUP ingestion must run inside an asyncio task")
        await self.checkpoints.acquire_lock(
            "levelup",
            current_run_id,
            self.settings.levelup_lock_ttl_seconds,
        )
        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(current_run_id, stop_heartbeat, owner_task)
        )
        try:
            await self.checkpoints.start_run(current_run_id, "levelup")
            await self.client.authenticate()
            ingestion_date = datetime.now(
                ZoneInfo(self.settings.ingestion_timezone)
            ).date().isoformat()

            if not await self.checkpoints.is_catalog_completed(current_run_id):
                await ingest_course_catalog(
                    self.settings,
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                )

            known_course_ids = await self.checkpoints.entity_keys(
                "levelup",
                COURSE_CATALOG,
            )
            await self.checkpoints.add_courses(current_run_id, known_course_ids)
            course_ids = await self.checkpoints.courses_to_process(current_run_id)
            results = await ingest_learning_history(
                self.settings,
                self.client,
                self.checkpoints,
                self.writer,
                current_run_id,
                ingestion_date,
                course_ids,
            )
            failed = [result for result in results if not result.succeeded]
            has_terminal_failures = await self.checkpoints.has_terminal_course_failures(
                current_run_id
            )
            if failed or has_terminal_failures:
                message = f"{len(failed)} LevelUP course(s) failed in this run"
                return await self.checkpoints.finish_run(
                    current_run_id,
                    RunStatus.PARTIAL_FAILURE,
                    message,
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is not None:
                message = sanitize_text(
                    self._heartbeat_error, self.client.sensitive_values()
                )
                logger.error(
                    "LevelUP lock heartbeat failed run_id=%s error=%s",
                    current_run_id,
                    message,
                )
                await asyncio.shield(
                    self.checkpoints.finish_run(
                        current_run_id,
                        RunStatus.FAILED,
                        message,
                    )
                )
            raise
        except Exception as exc:
            message = sanitize_text(exc, self.client.sensitive_values())
            logger.error(
                "LevelUP ingestion failed run_id=%s error=%s", current_run_id, message
            )
            return await self.checkpoints.finish_run(
                current_run_id,
                RunStatus.FAILED,
                message,
            )
        finally:
            stop_heartbeat.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await self.checkpoints.release_lock("levelup", current_run_id)

    async def _heartbeat_loop(
        self,
        run_id: str,
        stop: asyncio.Event,
        owner_task: asyncio.Task[Any],
    ) -> None:
        interval = min(60.0, max(1.0, self.settings.levelup_lock_ttl_seconds / 3))
        while not stop.is_set():
            try:
                async with asyncio.timeout(interval):
                    await stop.wait()
                return
            except TimeoutError:
                try:
                    await self.checkpoints.heartbeat_lock("levelup", run_id)
                except Exception as exc:
                    self._heartbeat_error = exc
                    owner_task.cancel()
                    return


async def run_levelup_ingestion(
    settings: Settings,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RunSummary:
    settings.validate_levelup_runtime()
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs(
        "levelup", settings.checkpoint_retention_days
    )
    if purged_runs:
        logger.info("Purged %d expired LevelUP checkpoint run(s)", purged_runs)
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        client = LevelUpClient(settings, http_client, sleep=sleep)
        job = LevelUpJob(settings, client, store, writer)
        return await job.run()
