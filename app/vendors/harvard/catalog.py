from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from app.config import Settings
from app.helpers.security import sanitize_text
from app.models import PageWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.harvard.catalog_client import (
    HarvardCatalogClient,
    HarvardCatalogContractError,
    is_retryable_error,
)
from app.vendors.harvard.models import HarvardVendorConfig

DOMAIN = "course_catalog"


async def ingest_catalog(
    settings: Settings,
    vendor: HarvardVendorConfig,
    client: HarvardCatalogClient,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    start_date: str | None = None,
) -> None:
    start = 0
    received = 0
    path = f"/api/catalog/{quote(vendor.org_key, safe='')}"
    while True:
        params: dict[str, Any] = {
            "catalogs": vendor.catalog_code,
            "start": start,
            "limit": settings.harvard_page_size,
        }
        if start_date is not None:
            params["startDate"] = start_date
        try:
            payload, raw_payload = await client.get_json(path, params)
            items = payload.get("list")
            records_count = len(items) if isinstance(items, list) else 0
            await writer.write_page(
                PageWrite(
                    vendor=vendor.vendor,
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    offset=start,
                    raw_payload=raw_payload,
                    records_count=records_count,
                    request_parameters=params,
                    fetched_at=datetime.now(UTC),
                )
            )
            if not isinstance(items, list):
                raise HarvardCatalogContractError(
                    "Harvard catalog response field 'list' must be an array"
                )
            await checkpoints.record_completed_page(
                run_id, DOMAIN, start, records_count
            )
            received += records_count
            total_count = payload.get("count")
            reached_total = (
                isinstance(total_count, int)
                and not isinstance(total_count, bool)
                and received >= total_count
            )
        except Exception as exc:
            message = sanitize_text(exc, client.sensitive_values())
            retryable = is_retryable_error(exc)
            await checkpoints.record_failed_page(
                run_id, DOMAIN, start, message, retryable=retryable
            )
            await checkpoints.mark_domain(
                run_id,
                DOMAIN,
                "retryable_failed" if retryable else "terminal_failed",
                message,
            )
            raise
        if not items or reached_total:
            break
        start += settings.harvard_page_size
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")
