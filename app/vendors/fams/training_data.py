from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.fams.client import (
    FAMSClient,
    FAMSResponseContractError,
    is_retryable_error,
)

DOMAIN = "training_data"
VENDOR = "fams"
logger = logging.getLogger(__name__)


def _record_counts(payload: Any) -> tuple[int, int]:
    if not isinstance(payload, dict):
        raise FAMSResponseContractError("FAMS response must be a JSON object")
    if payload.get("success") is not True:
        raise FAMSResponseContractError("FAMS response success is not true")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise FAMSResponseContractError("FAMS response data must be an object")
    class_list = data.get("classList")
    student_list = data.get("studentList")
    if not isinstance(class_list, list):
        raise FAMSResponseContractError(
            "FAMS response data.classList must be an array"
        )
    if not isinstance(student_list, list):
        raise FAMSResponseContractError(
            "FAMS response data.studentList must be an array"
        )
    return len(class_list), len(student_list)


async def ingest_training_data(
    client: FAMSClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    filters: Mapping[str, str] | None = None,
) -> None:
    offset = 1
    request_parameters = dict(filters or {})
    try:
        payload, raw_payload = await client.get_training_data(filters)
        validation_error: FAMSResponseContractError | None = None
        class_count = 0
        student_count = 0
        try:
            class_count, student_count = _record_counts(payload)
        except FAMSResponseContractError as exc:
            validation_error = exc

        await writer.write_page(
            PageWrite(
                vendor=VENDOR,
                data_domain=DOMAIN,
                ingestion_date=ingestion_date,
                run_id=run_id,
                offset=offset,
                raw_payload=raw_payload,
                records_count=class_count + student_count,
                request_parameters=request_parameters,
                fetched_at=datetime.now(UTC),
            )
        )
        if validation_error is not None:
            raise validation_error

        logger.info(
            "FAMS training data received class_count=%d student_count=%d",
            class_count,
            student_count,
        )
        await checkpoints.record_completed_page(
            run_id,
            DOMAIN,
            offset,
            class_count + student_count,
        )
        await checkpoints.mark_domain(run_id, DOMAIN, "completed")
    except Exception as exc:
        message = sanitize_text(exc, client.sensitive_values())
        retryable = is_retryable_error(exc)
        await checkpoints.record_failed_page(
            run_id,
            DOMAIN,
            offset,
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
