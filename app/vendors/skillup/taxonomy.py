from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.skillup.client import SkillUpClient, is_retryable_error

DOMAIN = "skill_taxonomy"


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
            items = payload.get("items")
            has_next_page = payload.get("hasNextPage")
            if not isinstance(items, list):
                raise ValueError("SkillUp taxonomy response must contain an items list")
            if not isinstance(has_next_page, bool):
                raise ValueError(
                    "SkillUp taxonomy response must contain boolean hasNextPage"
                )
            await writer.write_page(
                PageWrite(
                    vendor="skillup",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=page_number,
                    raw_payload=raw_payload,
                    records_count=len(items),
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, page_number, len(items)
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
        if not has_next_page:
            break
        page_number += 1
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")
