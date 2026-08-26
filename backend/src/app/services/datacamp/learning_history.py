from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.clients.datacamp_client import DataCampClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.datacamp import extra_field_paths, validate_events

DOMAIN = "learning_history"
VENDOR = "datacamp"
DAILY_SYNC_SCOPE = "daily_sync"
WEEKLY_SYNC_SCOPE = "weekly_sync"
FULL_SYNC_SCOPE = "full_sync"
logger = logging.getLogger(__name__)


async def ingest_learning_history(
    settings: Settings,
    client: DataCampClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    content_type: str | None = None,
    event_type: str | None = None,
    from_value: str | None = None,
    to: str | None = None,
    daily_sync_watermark: str | None = None,
    weekly_sync_watermark: str | None = None,
    full_sync_watermark: str | None = None,
) -> None:
    page = 1
    while True:
        params: dict[str, Any] = {
            "page": page,
            "pageSize": settings.datacamp_events_page_size,
        }
        if content_type is not None:
            params["contentType"] = content_type
        if event_type is not None:
            params["eventType"] = event_type
        if from_value is not None:
            params["from"] = from_value
        if to is not None:
            params["to"] = to
        try:
            payload, raw_payload = await client.get_json("/v1/events", params)
            contract = validate_events(
                payload,
                expected_page=page,
                expected_page_size=settings.datacamp_events_page_size,
            )
            records_count = len(contract.data)
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "DataCamp Learning History contains new contract fields fields=%s",
                    ",".join(extras),
                )
            await writer.write_page(
                PageWrite(
                    vendor="datacamp",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=page,
                    raw_payload=raw_payload,
                    records_count=records_count,
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, page, records_count
            )
        except Exception as exc:
            message = sanitize_text(exc, client.sensitive_values())
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id, DOMAIN, page, message, retryable=retryable
            )
            await checkpoints.mark_domain(
                run_id,
                DOMAIN,
                "retryable_failed" if retryable else "terminal_failed",
                message,
            )
            raise
        if page >= contract.meta.number_of_pages:
            break
        page += 1
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
