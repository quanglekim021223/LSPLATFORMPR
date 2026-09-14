from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime

from app.clients.datacamp_client import DataCampClient, is_retryable_error
from app.core.security import sanitize_text
from app.fabric_contract import records_from_bytes
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.datacamp import extra_field_paths, validate_archived_catalog
from app.services.record_delta import RecordDelta

DOMAIN = "course_catalog_archived"
CONTENT_FINGERPRINT_SCOPE = "content_fingerprint"
TABLE = "datacamp_course_catalog_archived"
logger = logging.getLogger(__name__)


async def ingest_archived_courses(
    client: DataCampClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    try:
        payload, raw_payload = await client.get_json("/v1/catalog/archived-courses")
        contract = validate_archived_catalog(payload)
        records_count = len(contract.data)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "DataCamp Archived Course Catalog contains new contract fields fields=%s",
                ",".join(extras),
            )
        fingerprint = hashlib.sha256(raw_payload).hexdigest()
        delta = await RecordDelta.load(checkpoints, "datacamp", TABLE)
        previous_fingerprint = await checkpoints.get_watermark(
            "datacamp",
            DOMAIN,
            CONTENT_FINGERPRINT_SCOPE,
        )
        if previous_fingerprint == fingerprint:
            if not delta.has_state:
                records = records_from_bytes(raw_payload, "data")
                selection = delta.select(records)
                if selection.indexes:
                    await delta.commit(selection, run_id)
            await checkpoints.record_completed_page(run_id, DOMAIN, 1, 0)
            await checkpoints.mark_domain(run_id, DOMAIN, "completed")
            return
        records = records_from_bytes(raw_payload, "data")
        selection = delta.select(records)
        if selection.indexes:
            await writer.write_page(
                PageWrite(
                    vendor="datacamp",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=1,
                    raw_payload=raw_payload,
                    records_count=len(selection.indexes),
                    source_records_count=records_count,
                    selected_record_indexes={"data": selection.indexes},
                    request_parameters={},
                    fetched_at=datetime.now(UTC),
                )
            )
            await delta.commit(selection, run_id)
        await checkpoints.record_completed_page(run_id, DOMAIN, 1, len(selection.indexes))
        await checkpoints.set_watermark(
            "datacamp",
            DOMAIN,
            fingerprint,
            run_id,
            CONTENT_FINGERPRINT_SCOPE,
        )
        await checkpoints.mark_domain(run_id, DOMAIN, "completed")
    except Exception as exc:
        message = sanitize_text(exc, client.sensitive_values())
        retryable = is_retryable_error(exc)
        await checkpoints.record_failed_page(run_id, DOMAIN, 1, message, retryable=retryable)
        await checkpoints.mark_domain(
            run_id,
            DOMAIN,
            "retryable_failed" if retryable else "terminal_failed",
            message,
        )
        raise
