from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.linkedin.client import LinkedInClient, is_retryable_error
from app.vendors.linkedin.models import extra_field_paths, validate_activity_reports
from app.vendors.linkedin.pagination import next_start

DOMAIN = "learning_history"
MAX_WINDOW_DAYS = 14
logger = logging.getLogger(__name__)


async def ingest_learning_history(
    settings: Settings,
    client: LinkedInClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    history_start = parse_history_start(settings.linkedin_history_start_time)
    history_end = datetime.now(UTC)
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
