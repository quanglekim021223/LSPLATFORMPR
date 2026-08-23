from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.skillup.client import SkillUpClient, is_retryable_error

DOMAIN = "assessment_history"


async def ingest_assessment_history(
    settings: Settings,
    client: SkillUpClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    include_sections: bool | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    page_number = await checkpoints.next_page_number(run_id, DOMAIN)
    while True:
        params: dict[str, Any] = {
            "PageNo": page_number,
            "PageSize": settings.skillup_page_size,
        }
        if include_sections is not None:
            params["includeSections"] = include_sections
        if start_date is not None:
            params["startDate"] = start_date
        if end_date is not None:
            params["endDate"] = end_date
        try:
            payload, raw_payload = await client.get_json(
                settings.skillup_reports_base_url, "/v3/reports", params
            )
            reports = payload.get("reports")
            if not isinstance(reports, list):
                raise ValueError(
                    "SkillUp assessment response must contain a reports list"
                )
            has_next_page = _has_next_page(
                payload, page_number, len(reports), settings.skillup_page_size
            )
            await writer.write_page(
                PageWrite(
                    vendor="skillup",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=page_number,
                    raw_payload=raw_payload,
                    records_count=len(reports),
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, page_number, len(reports)
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


def _has_next_page(
    payload: dict[str, Any], page_number: int, records_count: int, page_size: int
) -> bool:
    has_next_page = payload.get("hasNextPage")
    total_pages = payload.get("totalPages")
    if has_next_page is False:
        return False
    if isinstance(total_pages, int) and page_number >= total_pages:
        return False
    if has_next_page is True:
        return True
    if isinstance(total_pages, int):
        return page_number < total_pages
    return records_count >= page_size
