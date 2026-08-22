from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from urllib.parse import quote

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import CourseResult, PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.levelup.client import LevelUpClient, is_retryable_error
from app.vendors.levelup.pagination import is_last_page

logger = logging.getLogger(__name__)


async def ingest_learning_history(
    settings: Settings,
    client: LevelUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    course_ids: list[str],
) -> list[CourseResult]:
    semaphore = asyncio.Semaphore(settings.levelup_max_concurrency)
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    for course_id in course_ids:
        queue.put_nowait(course_id)
    worker_count = min(settings.levelup_max_concurrency, len(course_ids))
    for _ in range(worker_count):
        queue.put_nowait(None)
    results: list[CourseResult] = []

    async def worker() -> None:
        while True:
            course_id = await queue.get()
            try:
                if course_id is None:
                    return
                async with semaphore:
                    results.append(
                        await ingest_course(
                            settings,
                            client,
                            checkpoints,
                            writer,
                            run_id,
                            ingestion_date,
                            course_id,
                        )
                    )
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    if workers:
        await queue.join()
        await asyncio.gather(*workers)
    return results


async def ingest_course(
    settings: Settings,
    client: LevelUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    course_id: str,
) -> CourseResult:
    page_size = settings.levelup_page_size
    offset = await checkpoints.next_offset(
        run_id, "learning_history", page_size, course_id
    )
    result = CourseResult(course_id=course_id)
    try:
        while True:
            params = {"_limit": page_size, "_offset": offset}
            safe_course_id = quote(course_id, safe="-_.")
            path = (
                f"{settings.levelup_courses_path.rstrip('/')}"
                f"/{safe_course_id}/enrollments"
            )
            payload, raw_payload = await client.get_json(path, params)
            enrollments = payload.get("enrollments", [])
            if not isinstance(enrollments, list):
                raise ValueError(
                    "LevelUP enrollments response must contain an enrollments list"
                )
            fetched_at = datetime.now(UTC)
            await writer.write_page(
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
            await checkpoints.record_completed_page(
                run_id, "learning_history", offset, len(enrollments), course_id
            )
            result.records_count += len(enrollments)
            if is_last_page(payload, len(enrollments), offset, page_size):
                break
            offset += page_size
        await checkpoints.mark_course(run_id, course_id, "completed")
        return result
    except Exception as exc:
        result.succeeded = False
        result.retryable = is_retryable_error(exc)
        result.error_message = sanitize_text(exc, client.sensitive_values())
        await checkpoints.record_failed_page(
            run_id,
            "learning_history",
            offset,
            result.error_message,
            course_id,
            retryable=result.retryable,
        )
        await checkpoints.mark_course(
            run_id,
            course_id,
            "retryable_failed" if result.retryable else "terminal_failed",
            result.error_message,
        )
        logger.error(
            "LevelUP course failed run_id=%s course_id=%s error=%s",
            run_id,
            course_id,
            result.error_message,
        )
        return result
