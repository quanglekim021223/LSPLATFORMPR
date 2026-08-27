from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from app.clients.skillup_client import SkillUpClient
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import RunStatus, RunSummary
from app.repositories import BronzeWriter, CheckpointStore, LocalBronzeWriter
from app.services.skillup.assessment_history import (
    DAILY_SYNC_SCOPE,
    FULL_SYNC_SCOPE,
    WEEKLY_SYNC_SCOPE,
    ingest_assessment_history,
)
from app.services.skillup.assessment_history import (
    DOMAIN as ASSESSMENT_HISTORY,
)
from app.services.skillup.skill_inventory import DOMAIN as SKILL_INVENTORY
from app.services.skillup.skill_inventory import ingest_skill_inventory
from app.services.skillup.taxonomy import DOMAIN as SKILL_TAXONOMY
from app.services.skillup.taxonomy import ingest_skill_taxonomy

logger = logging.getLogger(__name__)

VENDOR = "skillup"
DOMAINS = (SKILL_TAXONOMY, SKILL_INVENTORY, ASSESSMENT_HISTORY)
LOCK_TTL_SECONDS = 3600


class SkillUpJob:
    def __init__(
        self,
        settings: Settings,
        client: SkillUpClient,
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
        taxonomy_params: Mapping[str, Any] | None = None,
        skill_profile_modified_since: str | None = None,
        search_text: str | None = None,
        include_sections: bool | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> RunSummary:
        current_run_id = str(uuid4())

        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("SkillUp ingestion must run inside an asyncio task")
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
                assessment_start,
                assessment_end,
                daily_sync_watermark,
                weekly_sync_watermark,
                full_sync_watermark,
            ) = await self._assessment_range(start_date, end_date)
            domains = set(await self.checkpoints.domains_to_process(current_run_id))
            tasks: list[Awaitable[None]] = []
            if SKILL_TAXONOMY in domains:
                tasks.append(
                    ingest_skill_taxonomy(
                        self.settings,
                        self.client,
                        self.checkpoints,
                        self.writer,
                        current_run_id,
                        ingestion_date,
                        taxonomy_params,
                    )
                )
            if SKILL_INVENTORY in domains:
                tasks.append(
                    ingest_skill_inventory(
                        self.settings,
                        self.client,
                        self.checkpoints,
                        self.writer,
                        current_run_id,
                        ingestion_date,
                        skill_profile_modified_since=skill_profile_modified_since,
                        search_text=search_text,
                    )
                )
            if ASSESSMENT_HISTORY in domains:
                tasks.append(
                    ingest_assessment_history(
                        self.settings,
                        self.client,
                        self.checkpoints,
                        self.writer,
                        current_run_id,
                        ingestion_date,
                        include_sections=include_sections,
                        start_date=assessment_start,
                        end_date=assessment_end,
                        daily_sync_watermark=daily_sync_watermark,
                        weekly_sync_watermark=weekly_sync_watermark,
                        full_sync_watermark=full_sync_watermark,
                    )
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [result for result in results if isinstance(result, BaseException)]
            has_terminal_failures = await self.checkpoints.has_terminal_domain_failures(
                current_run_id
            )
            if errors or has_terminal_failures:
                message = f"{len(errors)} SkillUp domain(s) failed in this run"
                status = (
                    RunStatus.PARTIAL_FAILURE
                    if len(errors) < len(tasks) or has_terminal_failures
                    else RunStatus.FAILED
                )
                return await self.checkpoints.finish_run(
                    current_run_id,
                    status,
                    message,
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is not None:
                message = sanitize_text(
                    self._heartbeat_error, self.client.sensitive_values()
                )
                logger.error(
                    "SkillUp lock heartbeat failed run_id=%s error=%s",
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
                "SkillUp ingestion failed run_id=%s error=%s", current_run_id, message
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
            await self.checkpoints.release_lock(VENDOR, current_run_id)

    async def _assessment_range(
        self,
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ]:
        if start_date is not None or end_date is not None:
            return start_date, end_date, None, None, None

        now = datetime.now(UTC)
        watermark = now.isoformat().replace("+00:00", "Z")
        last_full_sync = await self.checkpoints.get_watermark(
            VENDOR,
            ASSESSMENT_HISTORY,
            FULL_SYNC_SCOPE,
        )
        if _monthly_sync_due(
            last_full_sync,
            now,
            self.settings.ingestion_timezone,
        ):
            return (
                self.settings.skillup_assessment_start_date,
                watermark,
                watermark,
                watermark,
                watermark,
            )

        last_weekly_sync = await self.checkpoints.get_watermark(
            VENDOR,
            ASSESSMENT_HISTORY,
            WEEKLY_SYNC_SCOPE,
        )
        if _sync_due(
            last_weekly_sync,
            now,
            self.settings.skillup_assessment_weekly_sync_interval_days,
        ):
            lookback_start = max(
                _parse_utc(self.settings.skillup_assessment_start_date),
                now - timedelta(days=self.settings.skillup_assessment_lookback_days),
            )
            return (
                lookback_start.isoformat().replace("+00:00", "Z"),
                watermark,
                watermark,
                watermark,
                None,
            )

        last_daily_sync = await self.checkpoints.get_watermark(
            VENDOR,
            ASSESSMENT_HISTORY,
            DAILY_SYNC_SCOPE,
        )
        daily_start = max(
            _parse_utc(self.settings.skillup_assessment_start_date),
            _parse_utc(last_daily_sync or last_full_sync)
            - timedelta(days=self.settings.skillup_assessment_daily_overlap_days),
        )
        return (
            daily_start.isoformat().replace("+00:00", "Z"),
            watermark,
            watermark,
            None,
            None,
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
                    await self.checkpoints.heartbeat_lock(VENDOR, run_id)
                except Exception as exc:
                    self._heartbeat_error = exc
                    owner_task.cancel()
                    return


async def run_skillup_ingestion(
    settings: Settings,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    taxonomy_params: Mapping[str, Any] | None = None,
    skill_profile_modified_since: str | None = None,
    search_text: str | None = None,
    include_sections: bool | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> RunSummary:
    settings.validate_skillup_runtime()
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs(VENDOR, settings.checkpoint_retention_days)
    if purged_runs:
        logger.info("Purged %d expired SkillUp checkpoint run(s)", purged_runs)
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_read_timeout_seconds,
        pool=settings.http_connect_timeout_seconds,
    )
    async with httpx.AsyncClient(timeout=timeout, transport=transport) as http_client:
        client = SkillUpClient(settings, http_client, sleep=sleep)
        job = SkillUpJob(settings, client, store, writer)
        return await job.run(
            taxonomy_params=taxonomy_params,
            skill_profile_modified_since=skill_profile_modified_since,
            search_text=search_text,
            include_sections=include_sections,
            start_date=start_date,
            end_date=end_date,
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
        completed_at = _parse_utc(last_sync)
    except ValueError:
        return True
    zone = ZoneInfo(timezone)
    return completed_at.astimezone(zone).strftime("%Y-%m") != now.astimezone(
        zone
    ).strftime("%Y-%m")


def _parse_utc(value: str | None) -> datetime:
    if value is None:
        raise ValueError("SkillUp Assessment History watermark is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("SkillUp Assessment History watermark must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
