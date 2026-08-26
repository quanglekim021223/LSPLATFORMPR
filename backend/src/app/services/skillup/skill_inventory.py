from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.clients.skillup_client import SkillUpClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.skillup import extra_field_paths, validate_skill_inventory

DOMAIN = "skill_inventory"
VENDOR = "skillup"
logger = logging.getLogger(__name__)


async def ingest_skill_inventory(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    skill_profile_modified_since: str | None = None,
    search_text: str | None = None,
) -> None:
    managed_incremental = (
        skill_profile_modified_since is None and search_text is None
    )
    sync_watermark = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    previous_watermark = (
        await checkpoints.get_watermark(VENDOR, DOMAIN)
        if managed_incremental
        else None
    )
    page_number = await checkpoints.next_page_number(run_id, DOMAIN)
    while True:
        params: dict[str, Any] = {
            "pageNumber": page_number,
            "pageSize": settings.skillup_page_size,
        }
        modified_since = skill_profile_modified_since or previous_watermark
        if modified_since is not None:
            params["SkillProfileModifiedSince"] = modified_since
        if search_text is not None:
            params["searchText"] = search_text
        try:
            payload, raw_payload = await client.get_json(
                settings.skillup_intelligence_base_url,
                "/employees/skills-profile",
                params,
            )
            contract = validate_skill_inventory(payload)
            records_count = len(contract.items)
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "SkillUp Skill Inventory contains new contract fields fields=%s",
                    ",".join(extras),
                )
            await writer.write_page(
                PageWrite(
                    vendor="skillup",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=page_number,
                    raw_payload=raw_payload,
                    records_count=records_count,
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, page_number, records_count
            )
        except Exception as exc:
            message = sanitize_text(exc, client.sensitive_values())
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id, DOMAIN, page_number, message, retryable=retryable
            )
            await checkpoints.mark_domain(
                run_id,
                DOMAIN,
                "retryable_failed" if retryable else "terminal_failed",
                message,
            )
            raise
        if not contract.has_next_page:
            break
        page_number += 1
    if managed_incremental:
        await checkpoints.set_watermark(
            VENDOR,
            DOMAIN,
            sync_watermark,
            run_id,
        )
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")
