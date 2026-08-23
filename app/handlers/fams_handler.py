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

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import RunStatus, RunSummary
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter, LocalBronzeWriter
from app.vendors.fams.client import FAMSClient
from app.vendors.fams.training_data import DOMAIN, ingest_training_data

logger = logging.getLogger(__name__)

VENDOR = "fams"
DOMAINS = (DOMAIN,)


def configured_filters(settings: Settings) -> dict[str, str] | None:
    if settings.fams_load_mode == "full":
        return None
    values = {
        "status": settings.fams_status,
        "site": settings.fams_site,
        "actualStartDateFrom": settings.fams_actual_start_date_from,
        "actualStartDateTo": settings.fams_actual_start_date_to,
    }
    return {name: value for name, value in values.items() if value}


class FAMSJob:
    def __init__(
        self,
        settings: Settings,
        client: FAMSClient,
        checkpoint_store: CheckpointStore,
        bronze_writer: BronzeWriter,
    ) -> None:
        self.settings = settings
        self.client = client
        self.checkpoints = checkpoint_store
        self.writer = bronze_writer
        self._heartbeat_error: Exception | None = None

    async def run(self) -> RunSummary:
        run_id = str(uuid4())
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("FAMS ingestion must run inside an asyncio task")
        await self.checkpoints.acquire_lock(
            VENDOR,
            run_id,
            self.settings.fams_lock_ttl_seconds,
        )
        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(run_id, stop_heartbeat, owner_task)
        )
        try:
            await self.checkpoints.start_run(run_id, VENDOR)
            await self.checkpoints.add_domains(run_id, list(DOMAINS))
            self.settings.validate_fams_runtime()
            ingestion_date = datetime.now(
                ZoneInfo(self.settings.ingestion_timezone)
            ).date().isoformat()
            await ingest_training_data(
                self.client,
                self.checkpoints,
                self.writer,
                run_id,
                ingestion_date,
                configured_filters(self.settings),
            )
            return await self.checkpoints.finish_run(run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(
                self._heartbeat_error,
                self.client.sensitive_values(),
            )
            logger.error(
                "FAMS lock heartbeat failed run_id=%s error=%s",
                run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                run_id,
                RunStatus.FAILED,
                message,
            )
        except Exception as exc:
            message = sanitize_text(exc, self.client.sensitive_values())
            logger.error("FAMS ingestion failed run_id=%s error=%s", run_id, message)
            return await self.checkpoints.finish_run(
                run_id,
                RunStatus.FAILED,
                message,
            )
        finally:
            stop_heartbeat.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await self.checkpoints.release_lock(VENDOR, run_id)

    async def _heartbeat_loop(
        self,
        run_id: str,
        stop: asyncio.Event,
        owner_task: asyncio.Task[Any],
    ) -> None:
        interval = min(
            60.0,
            max(1.0, self.settings.fams_lock_ttl_seconds / 3),
        )
        while not stop.is_set():
            try:
                async with asyncio.timeout(interval):
                    await stop.wait()
                return
            except TimeoutError:
                try:
                    await self.checkpoints.heartbeat_lock(VENDOR, run_id)
                except Exception as exc:
                    self._heartbeat_error = exc
                    owner_task.cancel()
                    return


async def run_fams_ingestion(
    settings: Settings,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RunSummary:
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs(VENDOR, settings.checkpoint_retention_days)
    if purged_runs:
        logger.info("Purged %d expired FAMS checkpoint run(s)", purged_runs)
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        client = FAMSClient(settings, http_client, sleep=sleep)
        job = FAMSJob(settings, client, store, writer)
        return await job.run()
