from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from app.clients.coursera_client import CourseraClient
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import CourseResult, RunStatus, RunSummary
from app.repositories import BronzeWriter, CheckpointStore, LocalBronzeWriter
from app.services.coursera.course_catalog import (
    CATALOG_DOMAIN,
    DETAIL_DOMAIN,
    ingest_catalog_pipeline,
)
from app.services.coursera.learning_history import (
    DAILY_SYNC_SCOPE,
    FULL_SYNC_SCOPE,
    WEEKLY_SYNC_SCOPE,
    ingest_learning_history,
)
from app.services.coursera.learning_history import DOMAIN as LEARNING_HISTORY

logger = logging.getLogger(__name__)

VENDOR = "coursera"
DOMAINS = (CATALOG_DOMAIN, DETAIL_DOMAIN, LEARNING_HISTORY)
WEEKLY_SYNC_INTERVAL_DAYS = 7


class CourseraJob:
    def __init__(
        self,
        settings: Settings,
        client: CourseraClient,
        checkpoint_store: CheckpointStore,
        bronze_writer: BronzeWriter,
    ) -> None:
        self.settings = settings
        self.client = client
        self.checkpoints = checkpoint_store
        self.writer = bronze_writer
        self._heartbeat_error: Exception | None = None

    async def run(self) -> RunSummary:
        current_run_id = str(uuid4())
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("Coursera ingestion must run inside an asyncio task")
        await self.checkpoints.acquire_lock(
            VENDOR, current_run_id, self.settings.coursera_lock_ttl_seconds
        )
        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(current_run_id, stop_heartbeat, owner_task)
        )
        try:
            await self.checkpoints.start_run(current_run_id, VENDOR)
            await self.checkpoints.add_domains(current_run_id, list(DOMAINS))
            self.settings.validate_coursera_runtime()
            self.client.content_detail_path("configuration-check")
            await self.client.authenticate()
            now = datetime.now(UTC)
            ingestion_date = now.astimezone(
                ZoneInfo(self.settings.ingestion_timezone)
            ).date().isoformat()
            catalog_watermark = str(int(now.timestamp()))
            previous_catalog_watermark = await self.checkpoints.get_watermark(
                VENDOR,
                CATALOG_DOMAIN,
            )
            (
                last_activity_after,
                daily_sync_watermark,
                weekly_sync_watermark,
                full_sync_watermark,
            ) = await self._history_sync_plan(now)
            tasks: list[Awaitable[object]] = [
                ingest_catalog_pipeline(
                    self.settings,
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                    modified_since_timestamp=_parse_epoch(
                        previous_catalog_watermark
                    ),
                    sync_watermark=catalog_watermark,
                ),
                ingest_learning_history(
                    self.settings,
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                    last_activity_after=last_activity_after,
                    daily_sync_watermark=daily_sync_watermark,
                    weekly_sync_watermark=weekly_sync_watermark,
                    full_sync_watermark=full_sync_watermark,
                ),
            ]
            catalog_result, history_result = await asyncio.gather(
                *tasks, return_exceptions=True
            )
            results = (catalog_result, history_result)
            errors = [result for result in results if isinstance(result, BaseException)]
            failed_details: list[CourseResult] = []
            if isinstance(catalog_result, list):
                failed_details = [
                    result for result in catalog_result if not result.succeeded
                ]
            if errors or failed_details:
                status = (
                    RunStatus.FAILED
                    if len(errors) == len(tasks)
                    else RunStatus.PARTIAL_FAILURE
                )
                message = (
                    f"{len(errors)} Coursera domain pipeline(s) and "
                    f"{len(failed_details)} Course Detail request(s) failed"
                )
                return await self.checkpoints.finish_run(
                    current_run_id, status, message
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(self._heartbeat_error, self.client.sensitive_values())
            logger.error(
                "Coursera lock heartbeat failed run_id=%s error=%s",
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id, RunStatus.FAILED, message
            )
        except Exception as exc:
            message = sanitize_text(exc, self.client.sensitive_values())
            logger.error(
                "Coursera ingestion failed run_id=%s error=%s",
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
            await self.checkpoints.release_lock(VENDOR, current_run_id)

    async def _history_sync_plan(
        self,
        now: datetime,
    ) -> tuple[int | None, str, str | None, str | None]:
        sync_watermark = str(int(now.timestamp() * 1000))
        last_full_sync = await self.checkpoints.get_watermark(
            VENDOR,
            LEARNING_HISTORY,
            FULL_SYNC_SCOPE,
        )
        if _monthly_sync_due(
            last_full_sync,
            now,
            self.settings.ingestion_timezone,
        ):
            return None, sync_watermark, sync_watermark, sync_watermark

        last_weekly_sync = await self.checkpoints.get_watermark(
            VENDOR,
            LEARNING_HISTORY,
            WEEKLY_SYNC_SCOPE,
        )
        if _sync_due(last_weekly_sync, now, WEEKLY_SYNC_INTERVAL_DAYS):
            lookback_start = now - timedelta(
                days=self.settings.coursera_history_lookback_days
            )
            return (
                int(lookback_start.timestamp() * 1000),
                sync_watermark,
                sync_watermark,
                None,
            )

        last_daily_sync = await self.checkpoints.get_watermark(
            VENDOR,
            LEARNING_HISTORY,
            DAILY_SYNC_SCOPE,
        )
        daily_anchor = _parse_epoch(last_daily_sync or last_full_sync)
        if daily_anchor is None:
            return None, sync_watermark, sync_watermark, sync_watermark
        overlap_milliseconds = (
            self.settings.coursera_history_daily_overlap_days * 24 * 60 * 60 * 1000
        )
        return (
            max(0, daily_anchor - overlap_milliseconds),
            sync_watermark,
            None,
            None,
        )

    async def _heartbeat_loop(
        self,
        run_id: str,
        stop: asyncio.Event,
        owner_task: asyncio.Task[Any],
    ) -> None:
        interval = min(
            60.0, max(1.0, self.settings.coursera_lock_ttl_seconds / 3)
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


async def run_coursera_ingestion(
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
        logger.info("Purged %d expired Coursera checkpoint run(s)", purged_runs)
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        client = CourseraClient(settings, http_client, sleep=sleep)
        return await CourseraJob(settings, client, store, writer).run()


def _parse_epoch(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _sync_due(last_sync: str | None, now: datetime, interval_days: int) -> bool:
    completed_at = _epoch_milliseconds_datetime(last_sync)
    return completed_at is None or now - completed_at >= timedelta(days=interval_days)


def _monthly_sync_due(
    last_sync: str | None,
    now: datetime,
    timezone: str,
) -> bool:
    completed_at = _epoch_milliseconds_datetime(last_sync)
    if completed_at is None:
        return True
    zone = ZoneInfo(timezone)
    return completed_at.astimezone(zone).strftime("%Y-%m") != now.astimezone(
        zone
    ).strftime("%Y-%m")


def _epoch_milliseconds_datetime(value: str | None) -> datetime | None:
    milliseconds = _parse_epoch(value)
    if milliseconds is None:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, UTC)
