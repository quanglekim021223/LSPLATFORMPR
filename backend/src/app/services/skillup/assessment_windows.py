"""Bound assessment queries so live API page caps cannot truncate a successful run."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.clients.skillup_client import SkillUpClient
from app.core.config import Settings
from app.fabric_contract import records_from_bytes
from app.models import PageWrite
from app.repositories import BronzeWriter, CheckpointStore
from app.services.skillup.page_progress import PageProgress

AssessmentPage = tuple[dict[str, Any], bytes, int]


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Assessment time must have a timezone")
    return parsed.astimezone(UTC)


async def ingest_windows(
    settings: Settings,
    client: SkillUpClient,
    store: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    start: str,
    end: str,
    include_sections: bool | None,
) -> None:
    sequence = 1
    lower, end_time = parse_time(start), parse_time(end)
    if lower > end_time:
        raise ValueError("Assessment start is after end")
    while lower <= end_time:
        upper = min(end_time, lower + timedelta(days=1) - timedelta(microseconds=1))
        sequence = await _commit_window(
            settings,
            client,
            store,
            writer,
            run_id,
            ingestion_date,
            include_sections,
            lower,
            upper,
            sequence,
        )
        lower = upper + timedelta(microseconds=1)


async def _collect_window(
    settings: Settings,
    client: SkillUpClient,
    lower: datetime,
    upper: datetime,
    include_sections: bool | None,
) -> list[AssessmentPage]:
    tracker = PageProgress()
    pages: list[AssessmentPage] = []
    identities: set[str] = set()
    # Stop before an unbounded loop. Split the time range if the page cap is hit.
    for page in range(1, 201):
        params = _window_params(settings, lower, upper, page, include_sections)
        payload, raw = await client.get_json(
            settings.skillup_reports_base_url, "/v3/reports", params
        )
        records = records_from_bytes(raw, "reports")
        page_number, total_count, has_next = _pagination_values(payload)
        tracker.observe(page, page_number, total_count, len(records), has_next)
        identities.update(_assessment_id(record) for record in records)
        pages.append((params, raw, len(records)))
        if not has_next:
            if len(identities) != total_count:
                raise ValueError("Assessment pages did not cover all advertised IDs")
            return pages
    raise ValueError("Assessment window reached the page cap")


def _window_params(
    settings: Settings,
    lower: datetime,
    upper: datetime,
    page: int,
    include_sections: bool | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "PageNo": page,
        "PageSize": min(settings.skillup_page_size, 50),
        "startDate": lower.isoformat(),
        "endDate": upper.isoformat(),
    }
    if include_sections is not None:
        params["includeSections"] = include_sections
    return params


def _pagination_values(payload: dict[str, Any]) -> tuple[int, int, bool]:
    page_number = payload.get("pageNumber")
    total_count = payload.get("totalCount")
    has_next = payload.get("hasNextPage")
    if type(page_number) is not int or type(total_count) is not int:
        raise ValueError("Invalid assessment pagination metadata")
    if type(has_next) is not bool:
        raise ValueError("Invalid assessment hasNextPage")
    return page_number, total_count, has_next


def _assessment_id(record: dict[str, Any]) -> str:
    identity = record.get("testInvitationId")
    if type(identity) not in (int, str) or identity == "":
        raise ValueError("Missing assessment identity")
    return str(identity)


async def _commit_window(
    settings: Settings,
    client: SkillUpClient,
    store: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    include_sections: bool | None,
    lower: datetime,
    upper: datetime,
    sequence: int,
) -> int:
    try:
        pages = await _collect_window(settings, client, lower, upper, include_sections)
    except ValueError:
        if upper - lower <= timedelta(minutes=15):
            raise
        midpoint = lower + (upper - lower) / 2
        sequence = await _commit_window(
            settings,
            client,
            store,
            writer,
            run_id,
            ingestion_date,
            include_sections,
            lower,
            midpoint,
            sequence,
        )
        return await _commit_window(
            settings,
            client,
            store,
            writer,
            run_id,
            ingestion_date,
            include_sections,
            midpoint + timedelta(microseconds=1),
            upper,
            sequence,
        )
    return await _write_pages(store, writer, run_id, ingestion_date, pages, sequence)


async def _write_pages(
    store: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    pages: list[AssessmentPage],
    sequence: int,
) -> int:
    for params, raw, count in pages:
        await writer.write_page(
            PageWrite(
                vendor="skillup",
                data_domain="assessment_history",
                run_id=run_id,
                ingestion_date=ingestion_date,
                offset=sequence,
                raw_payload=raw,
                records_count=count,
                request_parameters=params,
                fetched_at=datetime.now(UTC),
            )
        )
        await store.record_completed_page(run_id, "assessment_history", sequence, count)
        sequence += 1
    return sequence
