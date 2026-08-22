from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx

from app.checkpoint import CheckpointStore
from app.config import Settings
from app.http_client import LevelUpClient
from app.models import CourseResult, PageWrite, RunStatus, RunSummary
from app.security import sanitize_text
from app.storage import BronzeWriter, LocalBronzeWriter

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
        self._course_semaphore = asyncio.Semaphore(settings.levelup_max_concurrency)
        self._heartbeat_error: Exception | None = None

    async def run(self, run_id: str | None = None) -> RunSummary:
        self._heartbeat_error = None
        resumable_run_id = None
        if run_id is None:
            resumable_run_id = await self.checkpoints.find_resumable_run(
                "levelup", self.settings.levelup_lock_ttl_seconds
            )
        current_run_id = run_id or resumable_run_id or str(uuid4())
        UUID(current_run_id)
        existing = await self.checkpoints.get_run(current_run_id)
        if existing and existing.status == RunStatus.SUCCEEDED:
            return existing

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
            ingestion_date = (
                datetime.now(ZoneInfo(self.settings.ingestion_timezone)).date().isoformat()
            )

            if not await self.checkpoints.is_catalog_completed(current_run_id):
                await self._ingest_catalog(current_run_id, ingestion_date)

            course_ids = await self.checkpoints.courses_to_process(current_run_id)
            results = await self._ingest_learning_history(
                current_run_id, ingestion_date, course_ids
            )
            failed = [result for result in results if not result.succeeded]
            if failed:
                message = f"{len(failed)} LevelUP course(s) failed; checkpoint state is resumable"
                return await self.checkpoints.finish_run(
                    current_run_id, RunStatus.PARTIAL_FAILURE, message
                )
            return await self.checkpoints.finish_run(current_run_id, RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            if self._heartbeat_error is None:
                raise
            message = sanitize_text(self._heartbeat_error, self.client.sensitive_values())
            logger.error(
                "LevelUP lock heartbeat failed run_id=%s error=%s",
                current_run_id,
                message,
            )
            return await self.checkpoints.finish_run(
                current_run_id, RunStatus.FAILED, message
            )
        except Exception as exc:
            message = sanitize_text(exc, self.client.sensitive_values())
            logger.error("LevelUP ingestion failed run_id=%s error=%s", current_run_id, message)
            return await self.checkpoints.finish_run(current_run_id, RunStatus.FAILED, message)
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

    async def _ingest_catalog(self, run_id: str, ingestion_date: str) -> None:
        page_size = self.settings.levelup_page_size
        offset = await self.checkpoints.next_offset(
            run_id, "course_catalog", page_size
        )
        pages = 0
        while True:
            params = {
                "_limit": page_size,
                "_offset": offset,
                "_filter": "vendor ne 'LinkedIn Learning'",
            }
            try:
                payload, _raw_payload = await self.client.get_json(
                    self.settings.levelup_courses_path, params
                )
            except Exception as exc:
                await self.checkpoints.record_failed_page(
                    run_id,
                    "course_catalog",
                    offset,
                    sanitize_text(exc, self.client.sensitive_values()),
                )
                raise
            raw_courses = payload.get("courses", [])
            if not isinstance(raw_courses, list):
                raise ValueError("LevelUP courses response must contain a courses list")
            valid_courses = [course for course in raw_courses if self._include_course(course)]
            course_ids = [
                course_id
                for course in valid_courses
                if (course_id := self._course_id(course)) is not None
            ]

            # Bronze remains byte-for-byte raw. The client-side exclusion only controls
            # which course IDs feed the Learning History calls.
            fetched_at = datetime.now(UTC)
            await self.writer.write_page(
                PageWrite(
                    vendor="levelup",
                    data_domain="course_catalog",
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=offset,
                    raw_payload=_raw_payload,
                    records_count=len(raw_courses),
                    request_parameters=params,
                    fetched_at=fetched_at,
                )
            )
            await self.checkpoints.record_completed_page(
                run_id, "course_catalog", offset, len(raw_courses)
            )
            await self.checkpoints.add_courses(run_id, course_ids)
            pages += 1
            logger.info(
                "LevelUP catalog page stored run_id=%s page=%d offset=%d "
                "courses_received=%d courses_selected=%d",
                run_id,
                pages,
                offset,
                len(raw_courses),
                len(valid_courses),
            )
            if self._is_last_page(payload, len(raw_courses), offset, page_size):
                break
            offset += page_size
        await self.checkpoints.mark_catalog_completed(run_id)

    async def _ingest_learning_history(
        self, run_id: str, ingestion_date: str, course_ids: list[str]
    ) -> list[CourseResult]:
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        for course_id in course_ids:
            queue.put_nowait(course_id)
        worker_count = min(self.settings.levelup_max_concurrency, len(course_ids))
        for _ in range(worker_count):
            queue.put_nowait(None)
        results: list[CourseResult] = []

        async def worker() -> None:
            while True:
                course_id = await queue.get()
                try:
                    if course_id is None:
                        return
                    async with self._course_semaphore:
                        results.append(
                            await self._ingest_course(run_id, ingestion_date, course_id)
                        )
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        if workers:
            await queue.join()
            await asyncio.gather(*workers)
        return results

    async def _ingest_course(
        self, run_id: str, ingestion_date: str, course_id: str
    ) -> CourseResult:
        page_size = self.settings.levelup_page_size
        offset = await self.checkpoints.next_offset(
            run_id, "learning_history", page_size, course_id
        )
        result = CourseResult(course_id=course_id)
        try:
            while True:
                params = {"_limit": page_size, "_offset": offset}
                safe_course_id = quote(course_id, safe="-_.")
                path = (
                    f"{self.settings.levelup_courses_path.rstrip('/')}"
                    f"/{safe_course_id}/enrollments"
                )
                payload, raw_payload = await self.client.get_json(path, params)
                enrollments = payload.get("enrollments", [])
                if not isinstance(enrollments, list):
                    raise ValueError(
                        "LevelUP enrollments response must contain an enrollments list"
                    )
                fetched_at = datetime.now(UTC)
                await self.writer.write_page(
                    PageWrite(
                        vendor="levelup",
                        data_domain="learning_history",
                        ingestion_date=ingestion_date,
                        run_id=run_id,
                        course_id=course_id,
                        offset=offset,
                        raw_payload=raw_payload,
                        records_count=len(enrollments),
                        request_parameters=params,
                        fetched_at=fetched_at,
                    )
                )
                await self.checkpoints.record_completed_page(
                    run_id, "learning_history", offset, len(enrollments), course_id
                )
                result.records_count += len(enrollments)
                if self._is_last_page(payload, len(enrollments), offset, page_size):
                    break
                offset += page_size
            await self.checkpoints.mark_course(run_id, course_id, "completed")
            return result
        except Exception as exc:
            result.succeeded = False
            result.error_message = sanitize_text(exc, self.client.sensitive_values())
            await self.checkpoints.record_failed_page(
                run_id,
                "learning_history",
                offset,
                result.error_message,
                course_id,
            )
            await self.checkpoints.mark_course(
                run_id, course_id, "failed", result.error_message
            )
            logger.error(
                "LevelUP course failed run_id=%s course_id=%s error=%s",
                run_id,
                course_id,
                result.error_message,
            )
            return result

    @staticmethod
    def _include_course(course: object) -> bool:
        if not isinstance(course, dict):
            return False
        vendor = course.get("vendor")
        return not (
            isinstance(vendor, str) and vendor.strip().casefold() == "linkedin learning".casefold()
        )

    @staticmethod
    def _course_id(course: dict[str, Any]) -> str | None:
        value = course.get("id") or course.get("courseId")
        return str(value) if value is not None and str(value).strip() else None

    @staticmethod
    def _is_last_page(
        payload: dict[str, Any], records_returned: int, offset: int, page_size: int
    ) -> bool:
        total_items = payload.get("totalItems")
        returned_items = payload.get("returnedItems")
        if isinstance(total_items, int) and isinstance(returned_items, int):
            return returned_items == 0 or offset + returned_items >= total_items
        return records_returned < page_size


async def run_levelup_ingestion(
    settings: Settings,
    *,
    checkpoint_store: CheckpointStore | None = None,
    bronze_writer: BronzeWriter | None = None,
    run_id: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RunSummary:
    settings.validate_levelup_runtime()
    store = checkpoint_store or CheckpointStore(settings.checkpoint_db_path)
    writer = bronze_writer or LocalBronzeWriter(settings.bronze_local_path)
    await store.initialize()
    purged_runs = await store.purge_old_runs("levelup", settings.checkpoint_retention_days)
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
        return await job.run(run_id)
