from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from app.clients.harvard_catalog_client import (
    HarvardCatalogClient,
    is_retryable_error,
)
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.models.harvard import HarvardVendorConfig
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.harvard import extra_field_paths, validate_catalog

DOMAIN = "course_catalog"
logger = logging.getLogger(__name__)


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
            contract = validate_catalog(payload)
            items = contract.items
            records_count = len(items)
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "Harvard Catalog contains new contract fields "
                    "vendor=%s fields=%s",
                    vendor.vendor,
                    ",".join(extras),
                )
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
            await checkpoints.record_completed_page(
                run_id, DOMAIN, start, records_count
            )
            received += records_count
            reached_total = received >= contract.count
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
