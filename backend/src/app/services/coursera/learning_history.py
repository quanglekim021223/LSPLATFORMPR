from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from app.clients.coursera_client import CourseraClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.coursera import extra_field_paths, validate_learning_history
from app.services.coursera.pagination import next_start

DOMAIN = "learning_history"
logger = logging.getLogger(__name__)


async def ingest_learning_history(
    settings: Settings,
    client: CourseraClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    start = 0
    path = f"/{quote(settings.coursera_org_id, safe='')}/enrollmentReports"
    while True:
        params: dict[str, Any] = {
            "start": start,
            "limit": settings.coursera_page_size,
            "includeS12n": True,
        }
        try:
            payload, raw_payload = await client.get_json(path, params)
            contract = validate_learning_history(payload)
            elements = contract.elements
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "Coursera Learning History contains new contract fields fields=%s",
                    ",".join(extras),
                )
            await writer.write_page(
                PageWrite(
                    vendor="coursera",
                    data_domain=DOMAIN,
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
                run_id, DOMAIN, start, len(elements)
            )
            following_start = next_start(payload, start)
        except Exception as exc:
            message = sanitize_text(exc, client.sensitive_values())
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id, DOMAIN, start, message, retryable=retryable
            )
            await checkpoints.mark_domain(
                run_id,
                DOMAIN,
                "retryable_failed" if retryable else "terminal_failed",
                message,
            )
            raise
        if following_start is None:
            break
        start = following_start
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")
