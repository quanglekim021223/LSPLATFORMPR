from __future__ import annotations

import logging
from datetime import UTC, datetime

from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.datacamp.client import DataCampClient, is_retryable_error
from app.vendors.datacamp.models import extra_field_paths, validate_live_catalog

DOMAIN = "course_catalog_live"
logger = logging.getLogger(__name__)


async def ingest_live_courses(
    client: DataCampClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    try:
        payload, raw_payload = await client.get_json("/v1/catalog/live-courses")
        contract = validate_live_catalog(payload)
        records_count = len(contract.data)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "DataCamp Live Course Catalog contains new contract fields fields=%s",
                ",".join(extras),
            )
        await writer.write_page(
            PageWrite(
                vendor="datacamp",
                data_domain=DOMAIN,
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=1,
                raw_payload=raw_payload,
                records_count=records_count,
                request_parameters={},
                fetched_at=datetime.now(UTC),
            )
        )
        await checkpoints.record_completed_page(run_id, DOMAIN, 1, records_count)
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
