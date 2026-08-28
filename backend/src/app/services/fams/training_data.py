from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.clients.fams_client import (
    FAMSClient,
    FAMSResponseContractError,
    is_retryable_error,
)
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore

DOMAIN = "training_data"
VENDOR = "fams"
CONTENT_FINGERPRINT_SCOPE = "content_fingerprint"
logger = logging.getLogger(__name__)


def _validated_lists(payload: Any) -> tuple[list[Any], list[Any]]:
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
    return class_list, student_list


def _content_fingerprint(
    class_list: list[Any], student_list: list[Any]
) -> str:
    def canonical_record(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    canonical_payload = json.dumps(
        {
            "classList": sorted(class_list, key=canonical_record),
            "studentList": sorted(student_list, key=canonical_record),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical_payload).hexdigest()


def _fingerprint_scope(filters: Mapping[str, str] | None) -> str:
    if not filters:
        return f"{CONTENT_FINGERPRINT_SCOPE}:full"
    canonical_filters = json.dumps(
        dict(sorted(filters.items())),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    filter_hash = hashlib.sha256(canonical_filters).hexdigest()
    return f"{CONTENT_FINGERPRINT_SCOPE}:filtered:{filter_hash}"


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
        class_list, student_list = _validated_lists(payload)
        class_count = len(class_list)
        student_count = len(student_list)
        fingerprint = _content_fingerprint(class_list, student_list)
        fingerprint_scope = _fingerprint_scope(filters)
        previous_fingerprint = await checkpoints.get_watermark(
            VENDOR,
            DOMAIN,
            fingerprint_scope,
        )

        if previous_fingerprint == fingerprint:
            logger.info(
                "FAMS training data unchanged class_count=%d student_count=%d",
                class_count,
                student_count,
            )
            await checkpoints.record_completed_page(run_id, DOMAIN, offset, 0)
            await checkpoints.mark_domain(run_id, DOMAIN, "completed")
            return

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
        logger.debug(
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
        await checkpoints.set_watermark(
            VENDOR,
            DOMAIN,
            fingerprint,
            run_id,
            fingerprint_scope,
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
