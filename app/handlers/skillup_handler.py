from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
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
from app.vendors.skillup.assessment_history import (
    DOMAIN as ASSESSMENT_HISTORY,
)
from app.vendors.skillup.assessment_history import ingest_assessment_history
from app.vendors.skillup.client import SkillUpClient
from app.vendors.skillup.skill_inventory import DOMAIN as SKILL_INVENTORY
from app.vendors.skillup.skill_inventory import ingest_skill_inventory
from app.vendors.skillup.taxonomy import DOMAIN as SKILL_TAXONOMY
from app.vendors.skillup.taxonomy import ingest_skill_taxonomy

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
                        start_date=start_date,
                        end_date=end_date,
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
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(self._heartbeat_error, self.client.sensitive_values())
            logger.error(
                "SkillUp lock heartbeat failed run_id=%s error=%s",
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id,
                RunStatus.FAILED,
                message,
            )
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
