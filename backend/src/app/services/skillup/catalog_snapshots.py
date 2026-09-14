from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from app.clients.skillup_client import SkillUpClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.skillup import validate_snapshot_page
from app.services.record_delta import RecordDelta
from app.services.skillup.page_progress import PageProgress

VENDOR = "skillup"
LEARNING_RESOURCES = "learning_resources"
CERTIFICATES = "certificates"
CONTENT_FINGERPRINT_SCOPE = "content_fingerprint"
TABLES = {
    LEARNING_RESOURCES: "skillup_learning_resources",
    CERTIFICATES: "skillup_certificates",
}
logger = logging.getLogger(__name__)


async def ingest_learning_resources(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    await _ingest_snapshot(
        settings,
        client,
        checkpoints,
        writer,
        run_id,
        ingestion_date,
        domain=LEARNING_RESOURCES,
        path="/learning/materials",
        id_field="learningMaterialId",
        extra_params={"IncludeSkills": "true"},
    )


async def ingest_certificates(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
) -> None:
    await _ingest_snapshot(
        settings,
        client,
        checkpoints,
        writer,
        run_id,
        ingestion_date,
        domain=CERTIFICATES,
        path="/certificates",
        id_field="certificateId",
        extra_params={"IncludeSkills": "true", "activeOnly": "false"},
    )


async def _ingest_snapshot(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    domain: str,
    path: str,
    id_field: str,
    extra_params: dict[str, str],
) -> None:
    page_number = await checkpoints.next_page_number(run_id, domain)
    progress = PageProgress()
    fingerprints: list[tuple[str, str]] = []
    delta = await RecordDelta.load(checkpoints, VENDOR, TABLES[domain])
    seed_existing = not delta.has_state
    try:
        while True:
            params: dict[str, Any] = {
                **extra_params,
                "PageNumber": page_number,
                "PageSize": settings.skillup_page_size,
            }
            payload, raw_payload = await client.get_json(
                settings.skillup_intelligence_base_url,
                path,
                params,
            )
            contract = validate_snapshot_page(
                payload,
                id_field=id_field,
                requested_page=page_number,
                page_size=settings.skillup_page_size,
                contract_name=domain,
            )
            records_count = len(contract.items)
            progress.observe(
                page_number,
                contract.page_number or page_number,
                contract.total_count,
                records_count,
                contract.has_next_page,
            )
            fingerprint = _records_fingerprint(contract.items)
            scope = f"{CONTENT_FINGERPRINT_SCOPE}:{page_number}"
            previous = await checkpoints.get_watermark(VENDOR, domain, scope)
            changed = previous != fingerprint
            written_records = 0
            if changed:
                selection = delta.select(contract.items)
                if selection.indexes:
                    await writer.write_page(
                        PageWrite(
                            vendor=VENDOR,
                            data_domain=domain,
                            ingestion_date=ingestion_date,
                            run_id=run_id,
                            offset=page_number,
                            raw_payload=raw_payload,
                            records_count=len(selection.indexes),
                            source_records_count=records_count,
                            selected_record_indexes={"items": selection.indexes},
                            request_parameters=params,
                            fetched_at=datetime.now(UTC),
                        )
                    )
                    await delta.commit(selection, run_id)
                    written_records = len(selection.indexes)
            elif seed_existing:
                selection = delta.select(contract.items)
                if selection.indexes:
                    # Upgrade old page-only checkpoints without replaying Bronze.
                    await delta.commit(selection, run_id)
            await checkpoints.record_completed_page(
                run_id,
                domain,
                page_number,
                written_records,
            )
            fingerprints.append((scope, fingerprint))
            if not contract.has_next_page:
                break
            page_number += 1

        for scope, fingerprint in fingerprints:
            await checkpoints.set_watermark(
                VENDOR,
                domain,
                fingerprint,
                run_id,
                scope,
            )
        await checkpoints.mark_domain(run_id, domain, "completed")
    except Exception as exc:
        message = sanitize_text(exc, client.sensitive_values())
        retryable = is_retryable_error(exc)
        await checkpoints.record_failed_page(
            run_id,
            domain,
            page_number,
            message,
            retryable=retryable,
        )
        await checkpoints.mark_domain(
            run_id,
            domain,
            "retryable_failed" if retryable else "terminal_failed",
            message,
        )
        raise


def _records_fingerprint(records: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
