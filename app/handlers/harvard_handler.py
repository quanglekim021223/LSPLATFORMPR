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
from app.vendors.harvard.catalog import DOMAIN as CATALOG_DOMAIN
from app.vendors.harvard.catalog import ingest_catalog
from app.vendors.harvard.catalog_client import HarvardCatalogClient, is_retryable_error
from app.vendors.harvard.models import HarvardVendorConfig, SFTPTransport, vendor_config
from app.vendors.harvard.sftp_client import (
    DOMAIN as LEARNING_HISTORY,
)
from app.vendors.harvard.sftp_client import (
    AsyncSSHSFTPTransport,
    ingest_learning_history,
    is_retryable_sftp_error,
)

logger = logging.getLogger(__name__)

DOMAINS = (CATALOG_DOMAIN, LEARNING_HISTORY)
LOCK_TTL_SECONDS = 14400


class HarvardJob:
    def __init__(
        self,
        settings: Settings,
        vendor: HarvardVendorConfig,
        catalog_client: HarvardCatalogClient,
        checkpoint_store: CheckpointStore,
        bronze_writer: BronzeWriter,
        *,
        sftp_transport: SFTPTransport | None,
        now: Callable[[], datetime],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self.settings = settings
        self.vendor = vendor
        self.catalog_client = catalog_client
        self.checkpoints = checkpoint_store
        self.writer = bronze_writer
        self.sftp_transport = sftp_transport
        self.now = now
        self.sleep = sleep
        self._heartbeat_error: Exception | None = None

    async def run(self, *, start_date: str | None = None) -> RunSummary:
        current_run_id = str(uuid4())
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("Harvard ingestion must run inside an asyncio task")
        await self.checkpoints.acquire_lock(
            self.vendor.vendor, current_run_id, LOCK_TTL_SECONDS
        )
        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(current_run_id, stop_heartbeat, owner_task)
        )
        try:
            await self.checkpoints.start_run(current_run_id, self.vendor.vendor)
            await self.checkpoints.add_domains(current_run_id, list(DOMAINS))
            ingestion_date = self.now().date().isoformat()

            async def catalog_branch() -> None:
                try:
                    self.settings.validate_harvard_catalog_runtime(self.vendor.vendor)
                    await self.catalog_client.authenticate()
                except Exception as exc:
                    message = sanitize_text(exc, self.sensitive_values())
                    retryable = is_retryable_error(exc)
                    await self.checkpoints.record_failed_page(
                        current_run_id,
                        CATALOG_DOMAIN,
                        0,
                        message,
                        retryable=retryable,
                    )
                    await self.checkpoints.mark_domain(
                        current_run_id,
                        CATALOG_DOMAIN,
                        "retryable_failed" if retryable else "terminal_failed",
                        message,
                    )
                    raise
                await ingest_catalog(
                    self.settings,
                    self.vendor,
                    self.catalog_client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                    start_date=start_date,
                )

            async def history_branch() -> None:
                try:
                    self.settings.validate_harvard_sftp_runtime()
                    sftp = self.sftp_transport or AsyncSSHSFTPTransport(
                        self.settings, sleep=self.sleep
                    )
                except Exception as exc:
                    message = sanitize_text(exc, self.sensitive_values())
                    await self.checkpoints.record_failed_page(
                        current_run_id,
                        LEARNING_HISTORY,
                        1,
                        message,
                        retryable=False,
                    )
                    await self.checkpoints.mark_domain(
                        current_run_id,
                        LEARNING_HISTORY,
                        "terminal_failed",
                        message,
                    )
                    raise
                entered = False
                try:
                    async with sftp:
                        entered = True
                        await ingest_learning_history(
                            self.settings,
                            self.vendor,
                            sftp,
                            self.checkpoints,
                            self.writer,
                            current_run_id,
                            ingestion_date,
                            now=self.now,
                            sleep=self.sleep,
                        )
                except Exception as exc:
                    if entered:
                        raise
                    message = sanitize_text(exc, self.sensitive_values())
                    retryable = is_retryable_sftp_error(exc)
                    await self.checkpoints.record_failed_page(
                        current_run_id,
                        LEARNING_HISTORY,
                        0,
                        message,
                        retryable=retryable,
                    )
                    await self.checkpoints.mark_domain(
                        current_run_id,
                        LEARNING_HISTORY,
                        "retryable_failed" if retryable else "terminal_failed",
                        message,
                    )
                    raise

            tasks = [
                catalog_branch(),
                history_branch(),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [result for result in results if isinstance(result, BaseException)]
            if errors:
                status = (
                    RunStatus.FAILED
                    if len(errors) == len(tasks)
                    else RunStatus.PARTIAL_FAILURE
                )
                return await self.checkpoints.finish_run(
                    current_run_id,
                    status,
                    f"{len(errors)} {self.vendor.display_name} domain(s) failed",
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(self._heartbeat_error, self.sensitive_values())
            logger.error(
                "%s lock heartbeat failed run_id=%s error=%s",
                self.vendor.display_name,
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id, RunStatus.FAILED, message
            )
        except Exception as exc:
            message = sanitize_text(exc, self.sensitive_values())
            logger.error(
                "%s ingestion failed run_id=%s error=%s",
                self.vendor.display_name,
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id, RunStatus.FAILED, message
            )
        finally:
            stop_heartbeat.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await self.checkpoints.release_lock(self.vendor.vendor, current_run_id)

    def sensitive_values(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                self.catalog_client.sensitive_values()
                + self.settings.harvard_secrets(self.vendor.vendor)
            )
        )

    async def _heartbeat_loop(
        self,
        run_id: str,
        stop: asyncio.Event,
        owner_task: asyncio.Task[Any],
    ) -> None:
        interval = min(60.0, max(1.0, LOCK_TTL_SECONDS / 3))
        while not stop.is_set():
            try:
                async with asyncio.timeout(interval):
                    await stop.wait()
                return
            except TimeoutError:
                try:
                    await self.checkpoints.heartbeat_lock(
                        self.vendor.vendor, run_id
                    )
                except Exception as exc:
                    self._heartbeat_error = exc
                    owner_task.cancel()
                    return


async def run_harvard_ingestion(
    settings: Settings,
    vendor_name: str,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sftp_transport: SFTPTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now: Callable[[], datetime] | None = None,
    start_date: str | None = None,
) -> RunSummary:
    vendor = vendor_config(settings, vendor_name)
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs(
        vendor.vendor, settings.checkpoint_retention_days
    )
    if purged_runs:
        logger.info(
            "Purged %d expired %s checkpoint run(s)",
            purged_runs,
            vendor.display_name,
        )
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    clock = now or (lambda: datetime.now(ZoneInfo(settings.ingestion_timezone)))
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        catalog_client = HarvardCatalogClient(
            settings, vendor, http_client, sleep=sleep
        )
        job = HarvardJob(
            settings,
            vendor,
            catalog_client,
            store,
            writer,
            sftp_transport=sftp_transport,
            now=clock,
            sleep=sleep,
        )
        return await job.run(start_date=start_date)
