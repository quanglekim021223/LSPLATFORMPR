from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.clients.levelup_client import (
    LevelUpClient,
    ResponseContractError,
    is_retryable_error,
)
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.levelup import extra_field_paths, validate_course_list
from app.services.levelup.pagination import is_last_page

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

        try:
            contract = validate_course_list(payload)
        except ResponseContractError as exc:
            await checkpoints.record_failed_page(
                run_id,
                "course_catalog",
                offset,
                str(exc),
                retryable=False,
            )
            raise
        raw_courses = payload.get("courses") if isinstance(payload, dict) else None
        records_count = len(raw_courses) if isinstance(raw_courses, list) else 0

        # Only contract-valid responses enter Bronze; their original bytes stay unchanged.
        fetched_at = datetime.now(UTC)
        await writer.write_page(
            PageWrite(
                vendor="levelup",
                data_domain="course_catalog",
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=offset,
                raw_payload=raw_payload,
                records_count=records_count,
                request_parameters=params,
                fetched_at=fetched_at,
            )
        )
        assert isinstance(payload, dict)
        assert isinstance(raw_courses, list)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "LevelUP Course List contains new contract fields fields=%s",
                ",".join(extras),
            )
        valid_courses = [course for course in raw_courses if include_course(course)]
        course_ids = [
            course_id
            for course in valid_courses
            if (course_id := extract_course_id(course)) is not None
        ]

        await checkpoints.record_completed_page(
            run_id, "course_catalog", offset, records_count
        )
        await checkpoints.add_courses(run_id, course_ids)
        pages += 1
        logger.debug(
            "LevelUP catalog page stored run_id=%s page=%d offset=%d "
            "courses_received=%d courses_selected=%d",
            run_id,
            pages,
            offset,
            records_count,
            len(valid_courses),
        )
        if is_last_page(payload, records_count, offset, page_size):
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
