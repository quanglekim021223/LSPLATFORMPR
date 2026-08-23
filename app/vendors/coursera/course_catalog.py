from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import CourseResult, PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.coursera.client import CourseraClient, is_retryable_error
from app.vendors.coursera.pagination import next_start

CATALOG_DOMAIN = "course_catalog"
DETAIL_DOMAIN = "course_detail"
logger = logging.getLogger(__name__)


async def ingest_catalog_pipeline(
    settings: Settings,
    client: CourseraClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> list[CourseResult]:
    try:
        await ingest_course_list(
            settings, client, checkpoints, writer, run_id, ingestion_date
        )
    except Exception as exc:
        message = sanitize_text(exc, client.sensitive_values())
        await checkpoints.mark_domain(
            run_id, DETAIL_DOMAIN, "terminal_failed", f"Course List failed: {message}"
        )
        raise
    course_ids = await checkpoints.courses_to_process(run_id)
    return await ingest_course_details(
        settings,
        client,
        checkpoints,
        writer,
        run_id,
        ingestion_date,
        course_ids,
    )


async def ingest_course_list(
    settings: Settings,
    client: CourseraClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    start = 0
    path = f"/{quote(settings.coursera_org_id, safe='')}/contents"
    while True:
        params = {"start": start, "limit": settings.coursera_page_size}
        try:
            payload, raw_payload = await client.get_json(path, params)
            elements = _elements(payload, CATALOG_DOMAIN)
            await writer.write_page(
                PageWrite(
                    vendor="coursera",
                    data_domain=CATALOG_DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=start,
                    raw_payload=raw_payload,
                    records_count=len(elements),
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, CATALOG_DOMAIN, start, len(elements)
            )
            await checkpoints.add_courses(run_id, _content_ids(elements))
            following_start = next_start(payload, start)
        except Exception as exc:
            message = sanitize_text(exc, client.sensitive_values())
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id,
                CATALOG_DOMAIN,
                start,
                message,
                retryable=retryable,
            )
            await checkpoints.mark_domain(
                run_id,
                CATALOG_DOMAIN,
                "retryable_failed" if retryable else "terminal_failed",
                message,
            )
            raise
        if following_start is None:
            break
        start = following_start
    await checkpoints.mark_domain(run_id, CATALOG_DOMAIN, "completed")


async def ingest_course_details(
    settings: Settings,
    client: CourseraClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    course_ids: list[str],
) -> list[CourseResult]:
    semaphore = asyncio.Semaphore(settings.coursera_max_concurrency)

    async def bounded_detail(content_id: str) -> CourseResult:
        async with semaphore:
            return await ingest_course_detail(
                client,
                checkpoints,
                writer,
                run_id,
                ingestion_date,
                content_id,
            )

    results = await asyncio.gather(*(bounded_detail(item) for item in course_ids))
    failed = [result for result in results if not result.succeeded]
    if failed:
        status = (
            "terminal_failed"
            if any(not result.retryable for result in failed)
            else "retryable_failed"
        )
        await checkpoints.mark_domain(
            run_id,
            DETAIL_DOMAIN,
            status,
            f"{len(failed)} Coursera Course Detail request(s) failed",
        )
    else:
        await checkpoints.mark_domain(run_id, DETAIL_DOMAIN, "completed")
    return results


async def ingest_course_detail(
    client: CourseraClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    content_id: str,
) -> CourseResult:
    result = CourseResult(course_id=content_id)
    try:
        path = client.content_detail_path(content_id)
        payload, raw_payload = await client.get_json(path, {})
        elements = _elements(payload, DETAIL_DOMAIN)
        await writer.write_page(
            PageWrite(
                vendor="coursera",
                data_domain=DETAIL_DOMAIN,
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=1,
                course_id=content_id,
                raw_payload=raw_payload,
                records_count=len(elements),
                request_parameters={},
                fetched_at=datetime.now(UTC),
            )
        )
        await checkpoints.record_completed_page(
            run_id, DETAIL_DOMAIN, 1, len(elements), content_id
        )
        await checkpoints.mark_course(run_id, content_id, "completed")
        result.records_count = len(elements)
    except Exception as exc:
        result.succeeded = False
        result.retryable = is_retryable_error(exc)
        result.error_message = sanitize_text(exc, client.sensitive_values())
        await checkpoints.record_failed_page(
            run_id,
            DETAIL_DOMAIN,
            1,
            result.error_message,
            content_id,
            retryable=result.retryable,
        )
        await checkpoints.mark_course(
            run_id,
            content_id,
            "retryable_failed" if result.retryable else "terminal_failed",
            result.error_message,
        )
        logger.error(
            "Coursera Course Detail failed run_id=%s content_id=%s error=%s",
            run_id,
            content_id,
            result.error_message,
        )
    return result


def _elements(payload: dict[str, Any], domain: str) -> list[Any]:
    elements = payload.get("elements")
    if isinstance(elements, list):
        return elements
    logger.warning(
        "Coursera response elements is not a list domain=%s; records_count=0",
        domain,
    )
    return []


def _content_ids(elements: list[Any]) -> list[str]:
    result: list[str] = []
    for element in elements:
        value = element.get("contentId") if isinstance(element, dict) else None
        if value is not None and str(value).strip():
            result.append(str(value))
    return result
