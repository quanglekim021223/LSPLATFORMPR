from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.clients.skillup_client import SkillUpClient, is_retryable_error
from app.core.config import Settings
from app.core.security import sanitize_text
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.skillup import extra_field_paths, validate_assessment_history
from app.services.watermarks import store_optional_watermarks

DOMAIN = "assessment_history"
VENDOR = "skillup"
DAILY_SYNC_SCOPE = "daily_sync"
WEEKLY_SYNC_SCOPE = "weekly_sync"
FULL_SYNC_SCOPE = "full_sync"
logger = logging.getLogger(__name__)


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
    daily_sync_watermark: str | None = None,
    weekly_sync_watermark: str | None = None,
    full_sync_watermark: str | None = None,
) -> None:
    page_number = await checkpoints.next_page_number(run_id, DOMAIN)
    while True:
        params = _report_params(
            page_number,
            settings.skillup_page_size,
            include_sections=include_sections,
            start_date=start_date,
            end_date=end_date,
        )
        try:
            payload, raw_payload = await client.get_json(
                settings.skillup_reports_base_url, "/v3/reports", params
            )
            contract = validate_assessment_history(
                payload, require_sections=include_sections is True
            )
            records_count = len(contract.reports)
            extras = extra_field_paths(contract)
            if extras:
                logger.warning(
                    "SkillUp Assessment History contains new contract fields fields=%s",
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
    await store_optional_watermarks(
        checkpoints,
        VENDOR,
        DOMAIN,
        run_id,
        (
            (DAILY_SYNC_SCOPE, daily_sync_watermark),
            (WEEKLY_SYNC_SCOPE, weekly_sync_watermark),
            (FULL_SYNC_SCOPE, full_sync_watermark),
        ),
    )
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")


def _report_params(
    page_number: int,
    page_size: int,
    *,
    include_sections: bool | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"PageNo": page_number, "PageSize": page_size}
    optional = {
        "includeSections": include_sections,
        "startDate": start_date,
        "endDate": end_date,
    }
    params.update({key: value for key, value in optional.items() if value is not None})
    return params

