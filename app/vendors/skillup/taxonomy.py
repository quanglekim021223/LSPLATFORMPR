from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.skillup.client import SkillUpClient, is_retryable_error
from app.vendors.skillup.models import extra_field_paths, validate_skill_taxonomy

DOMAIN = "skill_taxonomy"
logger = logging.getLogger(__name__)


async def ingest_skill_taxonomy(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    optional_params: Mapping[str, Any] | None = None,
) -> None:
    page_number = await checkpoints.next_page_number(run_id, DOMAIN)
    while True:
        params = dict(optional_params or {})
        params.update(
            {"PageNumber": page_number, "PageSize": settings.skillup_page_size}
        )
        try:
            payload, raw_payload = await client.get_json(
                settings.skillup_intelligence_base_url, "/taxonomy", params
            )
            contract = validate_skill_taxonomy(payload)
            records_count = len(contract.items)
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "SkillUp Skill Taxonomy contains new contract fields fields=%s",
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
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")
