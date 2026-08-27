from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.linkedin_client import LinkedInClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.linkedin import extra_field_paths, validate_activity_reports
from app.services.linkedin.pagination import next_start

DOMAIN = "learning_history"
VENDOR = "linkedin"
DAILY_SYNC_SCOPE = "daily_sync"
WEEKLY_SYNC_SCOPE = "weekly_sync"
FULL_SYNC_SCOPE = "full_sync"
MAX_WINDOW_DAYS = 14
logger = logging.getLogger(__name__)


async def ingest_learning_history(
    settings: Settings,
    client: LinkedInClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    history_start: datetime | None = None,
    history_end: datetime | None = None,
    daily_sync_watermark: str | None = None,
    weekly_sync_watermark: str | None = None,
    full_sync_watermark: str | None = None,
) -> None:
    history_start = history_start or parse_history_start(
        settings.linkedin_history_start_time
    )
    history_end = history_end or datetime.now(UTC)
    if history_start > history_end:
        raise ValueError("LINKEDIN_HISTORY_START_TIME must not be in the future")

    page_sequence = 1
    window_start = history_start
    while window_start < history_end:
        remaining_days = math.ceil(
            (history_end - window_start).total_seconds() / timedelta(days=1).total_seconds()
        )
        duration_days = min(MAX_WINDOW_DAYS, max(1, remaining_days))
        start = 0
        while True:
            params: dict[str, Any] = {
                "q": "criteria",
                "aggregationCriteria.primary": "INDIVIDUAL",
                "aggregationCriteria.secondary": "CONTENT",
                "assetType": "COURSE",
                "contentSource": "LINKEDIN_LEARNING",
                "startedAt": int(window_start.timestamp() * 1000),
                "timeOffset.unit": "DAY",
                "timeOffset.duration": duration_days,
                "start": start,
                "count": settings.linkedin_page_size,
            }
            try:
                payload, raw_payload = await client.get_json(
                    "/learningActivityReports", params
                )
                contract = validate_activity_reports(
                    payload,
                    expected_start=start,
                    expected_count=settings.linkedin_page_size,
                )
                elements = contract.elements
                extras = extra_field_paths(contract)
                if extras:
                    logger.warning(
                        "LinkedIn response contains new contract fields "
                        "domain=%s fields=%s",
                        DOMAIN,
                        ",".join(extras),
                    )
                await writer.write_page(
                    PageWrite(
                        vendor="linkedin",
                        data_domain=DOMAIN,
                        ingestion_date=ingestion_date,
                        run_id=run_id,
                        offset=page_sequence,
                        raw_payload=raw_payload,
                        records_count=len(elements),
                        request_parameters=params,
                        fetched_at=datetime.now(UTC),
                    )
                )
                await checkpoints.record_completed_page(
                    run_id, DOMAIN, page_sequence, len(elements)
                )
                following_start = next_start(payload, start)
            except Exception as exc:
                message = sanitize_text(exc, client.sensitive_values())
                retryable = is_retryable_error(exc)
                await checkpoints.record_failed_page(
                    run_id,
                    DOMAIN,
                    page_sequence,
                    message,
                    retryable=retryable,
                )
                await checkpoints.mark_domain(
                    run_id,
                    DOMAIN,
                    "retryable_failed" if retryable else "terminal_failed",
                    message,
                )
                raise
            page_sequence += 1
            if following_start is None:
                break
            start = following_start
        window_start += timedelta(days=duration_days)
    if daily_sync_watermark is not None:
        await checkpoints.set_watermark(
            VENDOR,
            DOMAIN,
            daily_sync_watermark,
            run_id,
            DAILY_SYNC_SCOPE,
        )
    if weekly_sync_watermark is not None:
        await checkpoints.set_watermark(
            VENDOR,
            DOMAIN,
            weekly_sync_watermark,
            run_id,
            WEEKLY_SYNC_SCOPE,
        )
    if full_sync_watermark is not None:
        await checkpoints.set_watermark(
            VENDOR,
            DOMAIN,
            full_sync_watermark,
            run_id,
            FULL_SYNC_SCOPE,
        )
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")


def parse_history_start(value: str) -> datetime:
    if value.isdigit():
        return datetime.fromtimestamp(int(value) / 1000, UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            "LINKEDIN_HISTORY_START_TIME must be ISO-8601 or epoch milliseconds"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError("LINKEDIN_HISTORY_START_TIME must include a timezone")
    return parsed.astimezone(UTC)
