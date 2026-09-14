"""Raw catalog ingestion: validate envelopes/paging, preserve optional vendor fields.

Bronze does not require optional descriptions/images/skills to exist. Typed
business schemas remain available to the application and future Silver jobs.
"""

from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from urllib.parse import quote

from app.clients.coursera_client import COURSE_DETAIL_CONTENT_TYPES
from app.fabric_contract import records_from_bytes
from app.models import PageWrite
from app.services.coursera.pagination import next_start as coursera_next
from app.services.linkedin.pagination import next_start as linkedin_next


def next_catalog_page(vendor: str, payload: dict[str, Any], start: int, count: int) -> int | None:
    paging = payload.get("paging")
    if not isinstance(paging, dict):
        raise ValueError("Missing catalog paging metadata")
    following = (coursera_next if vendor == "coursera" else linkedin_next)(payload, start)
    total = paging.get("total")
    if total is not None:
        if type(total) is not int or total < 0:
            raise ValueError("Invalid catalog total")
        if following is None and start + count < total:
            raise ValueError("Catalog ended before advertised total")
    if following is not None and count == 0:
        raise ValueError("Empty catalog page advertises a next page")
    return following


def _coursera_detail_ids(records: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for record in records:
        changes = record.get("changes")
        if isinstance(changes, list) and changes and all(
            isinstance(change, dict) and change.get("changeType") == "REMOVED"
            for change in changes
        ):
            continue
        typed_id = record.get("id")
        content_id = record.get("contentId")
        if not isinstance(typed_id, str) or not typed_id:
            raise ValueError("Coursera catalog record is missing id")
        if not isinstance(content_id, str) or not content_id:
            raise ValueError("Coursera catalog record is missing contentId")
        if typed_id.partition("~")[0] in COURSE_DETAIL_CONTENT_TYPES:
            ids.append(content_id)
    return ids


async def _record_catalog_page(
    store: Any,
    vendor: str,
    run_id: str,
    start: int,
    records: list[dict[str, Any]],
) -> None:
    await store.record_completed_page(run_id, "course_catalog", start, len(records))
    if vendor == "coursera":
        await store.add_courses(run_id, _coursera_detail_ids(records))


async def ingest_raw_catalog(
    settings: Any,
    client: Any,
    store: Any,
    writer: Any,
    run_id: str,
    day: str,
    vendor: str,
    *,
    modified_since: int | None = None,
    sync_watermark: str | None = None,
) -> None:
    start = 0
    seen: set[str] = set()
    path = (
        f"/{quote(settings.coursera_org_id, safe='')}/contents"
        if vendor == "coursera"
        else "/learningAssets"
    )
    while True:
        params = (
            {"start": start, "limit": settings.coursera_page_size}
            if vendor == "coursera"
            else {
                "q": "criteria",
                "assetFilteringCriteria.assetTypes[0]": "COURSE",
                "assetRetrievalCriteria.includeRetired": True,
                "start": start,
                "count": settings.linkedin_page_size,
            }
        )
        if modified_since is not None:
            filter_name = (
                "modifiedSinceTimestamp"
                if vendor == "coursera"
                else "assetFilteringCriteria.lastModifiedAfter"
            )
            params[filter_name] = modified_since
        payload, raw = await client.get_json(path, params)
        records = records_from_bytes(raw, "elements")
        digest = sha256(raw).hexdigest()
        if digest in seen:
            raise ValueError("Repeated catalog page")
        seen.add(digest)
        following = next_catalog_page(vendor, payload, start, len(records))
        await writer.write_page(
            PageWrite(
                vendor=vendor,
                data_domain="course_catalog",
                ingestion_date=day,
                run_id=run_id,
                offset=start,
                raw_payload=raw,
                records_count=len(records),
                request_parameters=params,
                fetched_at=datetime.now(UTC),
            )
        )
        await _record_catalog_page(store, vendor, run_id, start, records)
        if following is None:
            break
        start = following
    await store.mark_domain(run_id, "course_catalog", "completed")
    if sync_watermark is not None:
        await store.set_watermark(vendor, "course_catalog", sync_watermark, run_id)
