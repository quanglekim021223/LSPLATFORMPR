from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.datacamp.client import DataCampClient, is_retryable_error

DOMAIN = "learning_history"


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
            if not isinstance(payload, dict):
                raise ValueError("DataCamp events response must be a JSON object")
            meta = payload.get("meta")
            if not isinstance(meta, dict):
                raise ValueError("DataCamp events response must contain a meta object")
            number_of_pages = meta.get("numberOfPages")
            if (
                isinstance(number_of_pages, bool)
                or not isinstance(number_of_pages, int)
                or number_of_pages < 0
            ):
                raise ValueError(
                    "DataCamp events meta.numberOfPages must be a non-negative integer"
                )
            records_count = _records_count(payload)
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
        if page >= number_of_pages:
            break
        page += 1
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")


def _records_count(payload: dict[str, Any]) -> int:
    for key in ("events", "data"):
        records = payload.get(key)
        if isinstance(records, list):
            return len(records)
    return 0
