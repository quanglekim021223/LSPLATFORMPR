from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from urllib.parse import quote

import httpx

from app.clients.levelup_client import (
    LevelUpClient,
    ResponseContractError,
    is_retryable_error,
)
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import CourseResult, PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.levelup import extra_field_paths, validate_enrollments
from app.services.levelup.pagination import (
    incremental_filter,
    is_last_page,
    latest_timestamp,
)

logger = logging.getLogger(__name__)
VENDOR = "levelup"
DOMAIN = "learning_history"
CATALOG_DOMAIN = "course_catalog"


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
    watermark = await checkpoints.get_watermark(VENDOR, DOMAIN, course_id)
    offset = await checkpoints.next_offset(
        run_id, DOMAIN, page_size, course_id
    )
    result = CourseResult(course_id=course_id)
    received_timestamps: list[str] = []
    try:
        while True:
            params = _enrollment_params(page_size, offset, watermark)
            safe_course_id = quote(course_id, safe="-_.")
            path = (
                f"{settings.levelup_courses_path.rstrip('/')}"
                f"/{safe_course_id}/enrollments"
            )
            payload, raw_payload = await client.get_json(path, params)
            if not isinstance(payload, dict):
                raise ResponseContractError(
                    "LevelUP Enrollments response must be a JSON object"
                )
            contract = validate_enrollments(payload)
            enrollments = payload.get("enrollments")
            records_count = len(enrollments) if isinstance(enrollments, list) else 0
            fetched_at = datetime.now(UTC)
            await writer.write_page(
                PageWrite(
                    vendor=VENDOR,
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    course_id=course_id,
                    offset=offset,
                    raw_payload=raw_payload,
                    records_count=records_count,
                    request_parameters=params,
                    fetched_at=fetched_at,
                )
            )
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "LevelUP Enrollments contains new contract fields fields=%s",
                    ",".join(extras),
                )
            received_timestamps.extend(
                enrollment.date_edited for enrollment in contract.enrollments
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, offset, records_count, course_id
            )
            result.records_count += records_count
            if is_last_page(payload, records_count, offset, page_size):
                break
            offset += page_size
        await _complete_course(
            checkpoints, run_id, course_id, received_timestamps
        )
        return result
    except Exception as exc:
        if await _deactivate_missing_course(checkpoints, run_id, course_id, exc):
            await checkpoints.mark_course(run_id, course_id, "completed")
            logger.warning(
                "LevelUP course no longer exists; deactivated course_id=%s",
                course_id,
            )
            return result
        result.succeeded = False
        result.retryable = is_retryable_error(exc)
        result.error_message = sanitize_text(exc, client.sensitive_values())
        await checkpoints.record_failed_page(
            run_id,
            DOMAIN,
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


async def _complete_course(
    checkpoints: CheckpointStore,
    run_id: str,
    course_id: str,
    received_timestamps: list[str],
) -> None:
    await checkpoints.mark_course(run_id, course_id, "completed")
    next_watermark = latest_timestamp(received_timestamps)
    if next_watermark is not None:
        await checkpoints.set_watermark(
            VENDOR, DOMAIN, next_watermark, run_id, course_id
        )


def _enrollment_params(
    page_size: int, offset: int, watermark: str | None
) -> dict[str, int | str]:
    params: dict[str, int | str] = {
        "_limit": page_size,
        "_offset": offset,
        "_sort": "dateEdited",
    }
    if watermark is not None:
        params["_filter"] = incremental_filter(watermark)
    return params


async def _deactivate_missing_course(
    checkpoints: CheckpointStore,
    run_id: str,
    course_id: str,
    exc: Exception,
) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    if exc.response.status_code != httpx.codes.NOT_FOUND:
        return False
    return await checkpoints.deactivate_entity_key_if_stale(
        VENDOR, CATALOG_DOMAIN, course_id, run_id
    )
