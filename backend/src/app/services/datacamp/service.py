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

from app.clients.datacamp_client import DataCampClient
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import RunStatus, RunSummary
from app.repositories import BronzeWriter, CheckpointStore, LocalBronzeWriter
from app.services.datacamp.archived_courses import DOMAIN as ARCHIVED_COURSES
from app.services.datacamp.archived_courses import ingest_archived_courses
from app.services.datacamp.learning_history import (
    DAILY_SYNC_SCOPE,
    FULL_SYNC_SCOPE,
    WEEKLY_SYNC_SCOPE,
    ingest_learning_history,
)
from app.services.datacamp.learning_history import DOMAIN as LEARNING_HISTORY
from app.services.datacamp.live_courses import DOMAIN as LIVE_COURSES
from app.services.datacamp.live_courses import ingest_live_courses

logger = logging.getLogger(__name__)

VENDOR = "datacamp"
DOMAINS = (LIVE_COURSES, ARCHIVED_COURSES, LEARNING_HISTORY)
LOCK_TTL_SECONDS = 3600
WEEKLY_SYNC_INTERVAL_DAYS = 7


class DataCampJob:
    def __init__(
        self,
        settings: Settings,
        client: DataCampClient,
        checkpoint_store: CheckpointStore,
        bronze_writer: BronzeWriter,
    ) -> None:
        self.settings = settings
        self.client = client
        self.checkpoints = checkpoint_store
        self.writer = bronze_writer
        self._heartbeat_error: Exception | None = None

    async def run(
        self,
        *,
        content_type: str | None = None,
        event_type: str | None = None,
        from_value: str | None = None,
        to: str | None = None,
    ) -> RunSummary:
        current_run_id = str(uuid4())
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("DataCamp ingestion must run inside an asyncio task")
        await self.checkpoints.acquire_lock(VENDOR, current_run_id, LOCK_TTL_SECONDS)
        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(current_run_id, stop_heartbeat, owner_task)
        )
        try:
            await self.checkpoints.start_run(current_run_id, VENDOR)
            await self.checkpoints.add_domains(current_run_id, list(DOMAINS))
            ingestion_date = datetime.now(
                ZoneInfo(self.settings.ingestion_timezone)
            ).date().isoformat()
            (
                history_from,
                history_to,
                daily_sync_watermark,
                weekly_sync_watermark,
                full_sync_watermark,
            ) = await self._history_range(from_value, to)
            tasks: list[Awaitable[None]] = [
                ingest_live_courses(
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                ),
                ingest_archived_courses(
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                ),
                ingest_learning_history(
                    self.settings,
                    self.client,
                    self.checkpoints,
                    self.writer,
                    current_run_id,
                    ingestion_date,
                    content_type=content_type,
                    event_type=event_type,
                    from_value=history_from,
                    to=history_to,
                    daily_sync_watermark=daily_sync_watermark,
                    weekly_sync_watermark=weekly_sync_watermark,
                    full_sync_watermark=full_sync_watermark,
                ),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [result for result in results if isinstance(result, BaseException)]
            has_terminal_failures = await self.checkpoints.has_terminal_domain_failures(
                current_run_id
            )
            if errors or has_terminal_failures:
                status = (
                    RunStatus.PARTIAL_FAILURE
                    if len(errors) < len(tasks) or has_terminal_failures
                    else RunStatus.FAILED
                )
                return await self.checkpoints.finish_run(
                    current_run_id,
                    status,
                    f"{len(errors)} DataCamp domain(s) failed in this run",
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(self._heartbeat_error, self.client.sensitive_values())
            logger.error(
                "DataCamp lock heartbeat failed run_id=%s error=%s",
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id, RunStatus.FAILED, message
            )
        except Exception as exc:
            message = sanitize_text(exc, self.client.sensitive_values())
            logger.error(
                "DataCamp ingestion failed run_id=%s error=%s",
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

    async def _history_range(
        self,
        from_value: str | None,
        to: str | None,
    ) -> tuple[
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ]:
        if from_value is not None or to is not None:
            return from_value, to, None, None, None

        now = datetime.now(UTC)
        run_end = _utc_text(now)
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
            return (
                self.settings.datacamp_events_start_time,
                run_end,
                run_end,
                run_end,
                run_end,
            )

        last_weekly_sync = await self.checkpoints.get_watermark(
            VENDOR,
            LEARNING_HISTORY,
            WEEKLY_SYNC_SCOPE,
        )
        if _sync_due(last_weekly_sync, now, WEEKLY_SYNC_INTERVAL_DAYS):
            lookback_start = now - timedelta(
                days=self.settings.datacamp_events_lookback_days
            )
            return _utc_text(lookback_start), run_end, run_end, run_end, None

        last_daily_sync = await self.checkpoints.get_watermark(
            VENDOR,
            LEARNING_HISTORY,
            DAILY_SYNC_SCOPE,
        )
        return last_daily_sync or last_full_sync, run_end, run_end, None, None

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
                    await self.checkpoints.heartbeat_lock(VENDOR, run_id)
                except Exception as exc:
                    self._heartbeat_error = exc
                    owner_task.cancel()
                    return


async def run_datacamp_ingestion(
    settings: Settings,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    content_type: str | None = None,
    event_type: str | None = None,
    from_value: str | None = None,
    to: str | None = None,
) -> RunSummary:
    settings.validate_datacamp_runtime()
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs(VENDOR, settings.checkpoint_retention_days)
    if purged_runs:
        logger.info("Purged %d expired DataCamp checkpoint run(s)", purged_runs)
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        client = DataCampClient(settings, http_client, sleep=sleep)
        job = DataCampJob(settings, client, store, writer)
        return await job.run(
            content_type=content_type,
            event_type=event_type,
            from_value=from_value,
            to=to,
        )


def _sync_due(
    last_sync: str | None,
    now: datetime,
    interval_days: int,
) -> bool:
    if last_sync is None:
        return True
    try:
        completed_at = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
    except ValueError:
        return True
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=UTC)
    return now - completed_at >= timedelta(days=interval_days)


def _monthly_sync_due(
    last_sync: str | None,
    now: datetime,
    timezone: str,
) -> bool:
    if last_sync is None:
        return True
    try:
        completed_at = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
    except ValueError:
        return True
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=UTC)
    zone = ZoneInfo(timezone)
    completed_month = completed_at.astimezone(zone).strftime("%Y-%m")
    current_month = now.astimezone(zone).strftime("%Y-%m")
    return completed_month != current_month


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
