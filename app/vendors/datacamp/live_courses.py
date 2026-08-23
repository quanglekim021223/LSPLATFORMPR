from __future__ import annotations

from datetime import UTC, datetime

from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.datacamp.client import DataCampClient, is_retryable_error

DOMAIN = "course_catalog_live"


async def ingest_live_courses(
    client: DataCampClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    try:
        _, raw_payload = await client.get_json("/v1/catalog/live-courses")
        await writer.write_page(
            PageWrite(
                vendor="datacamp",
                data_domain=DOMAIN,
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=1,
                raw_payload=raw_payload,
                records_count=0,
                request_parameters={},
                fetched_at=datetime.now(UTC),
            )
        )
        await checkpoints.record_completed_page(run_id, DOMAIN, 1, 0)
        await checkpoints.mark_domain(run_id, DOMAIN, "completed")
    except Exception as exc:
        message = sanitize_text(exc, client.sensitive_values())
        retryable = is_retryable_error(exc)
        await checkpoints.record_failed_page(
            run_id, DOMAIN, 1, message, retryable=retryable
        )
        await checkpoints.mark_domain(
            run_id,
            DOMAIN,
            "retryable_failed" if retryable else "terminal_failed",
            message,
        )
        raise
