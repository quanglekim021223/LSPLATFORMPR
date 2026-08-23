from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.skillup.client import SkillUpClient, is_retryable_error

DOMAIN = "skill_inventory"


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
    page_number = await checkpoints.next_page_number(run_id, DOMAIN)
    while True:
        params: dict[str, Any] = {
            "pageNumber": page_number,
            "pageSize": settings.skillup_page_size,
        }
        if skill_profile_modified_since is not None:
            params["SkillProfileModifiedSince"] = skill_profile_modified_since
        if search_text is not None:
            params["searchText"] = search_text
        try:
            payload, raw_payload = await client.get_json(
                settings.skillup_intelligence_base_url,
                "/employees/skills-profile",
                params,
            )
            records = _extract_records(payload)
            has_next_page = _has_next_page(
                payload, page_number, len(records), settings.skillup_page_size
            )
            await writer.write_page(
                PageWrite(
                    vendor="skillup",
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=page_number,
                    raw_payload=raw_payload,
                    records_count=len(records),
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, page_number, len(records)
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


def _extract_records(payload: dict[str, Any]) -> list[Any]:
    for key in ("items", "employees", "skillProfiles", "profiles", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested_items = value.get("items")
            if isinstance(nested_items, list):
                return nested_items
    raise ValueError(
        "SkillUp inventory response must contain a supported records list"
    )


def _has_next_page(
    payload: dict[str, Any], page_number: int, records_count: int, page_size: int
) -> bool:
    metadata = payload
    for key in ("pagination", "metadata", "meta"):
        value = payload.get(key)
        if isinstance(value, dict):
            metadata = value
            break
    has_next_page = metadata.get("hasNextPage")
    if isinstance(has_next_page, bool):
        return has_next_page
    response_page = metadata.get("pageNumber", page_number)
    total_pages = metadata.get("totalPages")
    if isinstance(response_page, int) and isinstance(total_pages, int):
        return response_page < total_pages
    return records_count >= page_size
