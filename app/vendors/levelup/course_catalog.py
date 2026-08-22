from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.levelup.client import LevelUpClient, is_retryable_error
from app.vendors.levelup.pagination import is_last_page

logger = logging.getLogger(__name__)


async def ingest_course_catalog(
    settings: Settings,
    client: LevelUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    page_size = settings.levelup_page_size
    offset = await checkpoints.next_offset(run_id, "course_catalog", page_size)
    pages = 0
    while True:
        params = {
            "_limit": page_size,
            "_offset": offset,
            "_filter": "vendor ne 'LinkedIn Learning'",
        }
        try:
            payload, raw_payload = await client.get_json(settings.levelup_courses_path, params)
        except Exception as exc:
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id,
                "course_catalog",
                offset,
                sanitize_text(exc, client.sensitive_values()),
                retryable=retryable,
            )
            raise

        raw_courses = payload.get("courses", [])
        if not isinstance(raw_courses, list):
            raise ValueError("LevelUP courses response must contain a courses list")
        valid_courses = [course for course in raw_courses if include_course(course)]
        course_ids = [
            course_id
            for course in valid_courses
            if (course_id := extract_course_id(course)) is not None
        ]

        # The filtered list only feeds Learning History. Bronze stays byte-for-byte raw.
        fetched_at = datetime.now(UTC)
        await writer.write_page(
            PageWrite(
                vendor="levelup",
                data_domain="course_catalog",
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=offset,
                raw_payload=raw_payload,
                records_count=len(raw_courses),
                request_parameters=params,
                fetched_at=fetched_at,
            )
        )
        await checkpoints.record_completed_page(
            run_id, "course_catalog", offset, len(raw_courses)
        )
        await checkpoints.add_courses(run_id, course_ids)
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
        if is_last_page(payload, len(raw_courses), offset, page_size):
            break
        offset += page_size
    await checkpoints.mark_catalog_completed(run_id)


def include_course(course: object) -> bool:
    if not isinstance(course, dict):
        return False
    vendor = course.get("vendor")
    return not (
        isinstance(vendor, str)
        and vendor.strip().casefold() == "linkedin learning".casefold()
    )


def extract_course_id(course: dict[str, Any]) -> str | None:
    value = course.get("id") or course.get("courseId")
    return str(value) if value is not None and str(value).strip() else None
