from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.clients.fams_client import (
    FAMSClient,
    is_retryable_error,
)
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.fams import extra_field_paths, validate_training_data

DOMAIN = "training_data"
VENDOR = "fams"
CONTENT_FINGERPRINT_SCOPE = "content_fingerprint"
logger = logging.getLogger(__name__)


def _content_fingerprint(class_list: list[Any], student_list: list[Any]) -> str:
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
        contract = validate_training_data(payload)
        class_list = [
            item.model_dump(mode="json", by_alias=True) for item in contract.data.class_list
        ]
        student_list = [
            item.model_dump(mode="json", by_alias=True) for item in contract.data.student_list
        ]
        class_count = len(class_list)
        student_count = len(student_list)
        extras = extra_field_paths(contract)
        if extras:
            logger.warning(
                "FAMS Training Data contains new contract fields fields=%s",
                ",".join(extras),
            )
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
