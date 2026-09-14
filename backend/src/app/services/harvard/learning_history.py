from __future__ import annotations

import asyncio
import posixpath
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from app.core.config import Settings
from app.core.security import sanitize_text
from app.fabric_contract import records_from_bytes
from app.models import BinaryFileWrite
from app.models.harvard import (
    HarvardVendorConfig,
    RemoteFile,
    RemoteFileMetadata,
    SFTPTransport,
)
from app.repositories import BronzeWriter, CheckpointStore
from app.schemas.harvard import HarvardResponseContractError, validate_history_csv
from app.services.record_delta import RecordDelta

DOMAIN = "learning_history"


class HarvardHistoryIngestionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _Polling:
    now: Callable[[], datetime]
    sleep: Callable[[float], Awaitable[None]]


async def ingest_learning_history(
    settings: Settings,
    vendor: HarvardVendorConfig,
    transport: SFTPTransport,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    *,
    now: Callable[[], datetime],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    current = now()
    if current.tzinfo is None:
        raise ValueError("Harvard SFTP clock must be timezone-aware")
    last_report_date = current.date() - timedelta(days=settings.harvard_report_date_offset_days)
    first_report_date = await _validated_start_date(
        settings, vendor, checkpoints, run_id, last_report_date
    )
    listed_files = await _listed_files(settings, vendor, transport, checkpoints, run_id)
    metadata_by_path = {item.remote_path: item for item in listed_files}
    delta = await RecordDelta.load(
        checkpoints,
        vendor.vendor,
        f"{vendor.vendor}_learning_history",
    )
    seed_existing = not delta.has_state

    failures: list[tuple[str, bool]] = []
    polling = _Polling(now, sleep)
    report_dates = _report_dates(
        settings, vendor, listed_files, first_report_date, last_report_date
    )
    for report_date in report_dates:
        failure = await _ingest_report_date(
            settings,
            vendor,
            transport,
            checkpoints,
            writer,
            run_id,
            ingestion_date,
            report_date,
            last_report_date,
            metadata_by_path,
            delta,
            seed_existing,
            polling=polling,
        )
        if failure is not None:
            failures.append(failure)

    if failures:
        retryable = any(item[1] for item in failures)
        message = f"{len(failures)} Harvard Learning History file(s) failed"
        await checkpoints.mark_domain(
            run_id,
            DOMAIN,
            "retryable_failed" if retryable else "terminal_failed",
            message,
        )
        raise HarvardHistoryIngestionError(message)
    await checkpoints.mark_domain(run_id, DOMAIN, "completed")


async def _validated_start_date(
    settings: Settings,
    vendor: HarvardVendorConfig,
    checkpoints: CheckpointStore,
    run_id: str,
    last_report_date: date,
) -> date:
    try:
        return _history_start_date(settings, vendor, last_report_date)
    except ValueError as exc:
        message = sanitize_text(exc, settings.harvard_secrets(vendor.vendor))
        await checkpoints.record_failed_page(run_id, DOMAIN, 0, message, retryable=False)
        await checkpoints.mark_domain(run_id, DOMAIN, "terminal_failed", message)
        raise


async def _listed_files(
    settings: Settings,
    vendor: HarvardVendorConfig,
    transport: SFTPTransport,
    checkpoints: CheckpointStore,
    run_id: str,
) -> list[RemoteFileMetadata]:
    try:
        return await transport.list_files(settings.harvard_sftp_remote_dir)
    except Exception as exc:
        message = sanitize_text(exc, settings.harvard_secrets(vendor.vendor))
        retryable = not isinstance(exc, (PermissionError, TypeError, ValueError))
        await checkpoints.record_failed_page(run_id, DOMAIN, 0, message, retryable=retryable)
        status = "retryable_failed" if retryable else "terminal_failed"
        await checkpoints.mark_domain(run_id, DOMAIN, status, message)
        raise


def _report_dates(
    settings: Settings,
    vendor: HarvardVendorConfig,
    listed_files: list[RemoteFileMetadata],
    first_report_date: date,
    last_report_date: date,
) -> list[date]:
    if not settings.fabric_enabled:
        return [
            first_report_date + timedelta(days=offset)
            for offset in range((last_report_date - first_report_date).days + 1)
        ]
    return _available_report_dates(
        settings, vendor, listed_files, first_report_date, last_report_date
    )


def _available_report_dates(
    settings: Settings,
    vendor: HarvardVendorConfig,
    listed_files: list[RemoteFileMetadata],
    first_report_date: date,
    last_report_date: date,
) -> list[date]:
    # Live SFTP has gaps/retention; scan available files, including edited older files.
    dates: set[date] = set()
    prefix = vendor.report_filename_prefix
    explicit_start = (
        settings.harvard_hmm_history_start_date
        if vendor.vendor == "harvard_hmm"
        else settings.harvard_spark_history_start_date
    )
    for item in listed_files:
        name = posixpath.basename(item.remote_path)
        candidate = _report_date(name, prefix)
        if candidate is None or candidate > last_report_date:
            continue
        if not explicit_start or candidate >= first_report_date:
            dates.add(candidate)
    return sorted(dates)


def _report_date(file_name: str, prefix: str) -> date | None:
    if not file_name.startswith(prefix) or not file_name.endswith(".csv"):
        return None
    try:
        return datetime.strptime(file_name[len(prefix) : -4], "%Y%m%d").date()
    except ValueError:
        return None


async def _ingest_report_date(
    settings: Settings,
    vendor: HarvardVendorConfig,
    transport: SFTPTransport,
    checkpoints: CheckpointStore,
    writer: BronzeWriter,
    run_id: str,
    ingestion_date: str,
    report_date: date,
    last_report_date: date,
    metadata_by_path: dict[str, RemoteFileMetadata],
    delta: RecordDelta,
    seed_existing: bool,
    *,
    polling: _Polling,
) -> tuple[str, bool] | None:
    file_name = f"{vendor.report_filename_prefix}{report_date:%Y%m%d}.csv"
    remote_path = posixpath.join(settings.harvard_sftp_remote_dir, file_name)
    metadata = metadata_by_path.get(remote_path)
    source_is_current = await _source_file_is_current(
        checkpoints, vendor, remote_path, metadata
    )
    if source_is_current and not seed_existing:
        return None
    offset = int(report_date.strftime("%Y%m%d"))
    try:
        remote_file = await _fetch_report(
            settings,
            transport,
            remote_path,
            file_name,
            poll=report_date == last_report_date,
            now=polling.now,
            sleep=polling.sleep,
        )
        source_records_count = validate_history_csv(remote_file.content, vendor.vendor)
        records = records_from_bytes(remote_file.content, "csv")
        selection = delta.select(records)
        if not source_is_current and selection.indexes:
            await writer.write_file(
                BinaryFileWrite(
                    vendor=vendor.vendor,
                    data_domain=DOMAIN,
                    ingestion_date=ingestion_date,
                    run_id=run_id,
                    raw_payload=remote_file.content,
                    file_name=file_name,
                    remote_path=remote_file.remote_path,
                    file_size=remote_file.size,
                    remote_modified_time=remote_file.modified_at,
                    downloaded_at=datetime.now(UTC),
                    records_count=len(selection.indexes),
                    source_records_count=source_records_count,
                    selected_record_indexes={"csv": selection.indexes},
                )
            )
        if selection.indexes:
            await delta.commit(selection, run_id)
        records_count = 0 if source_is_current else len(selection.indexes)
        await checkpoints.record_completed_page(run_id, DOMAIN, offset, records_count)
        await checkpoints.record_completed_source_file(
            vendor.vendor,
            DOMAIN,
            remote_path,
            run_id,
            remote_file.size,
            remote_file.modified_at,
        )
        return None
    except Exception as exc:
        message = sanitize_text(exc, settings.harvard_secrets(vendor.vendor))
        retryable = not isinstance(
            exc,
            (FileNotFoundError, HarvardResponseContractError, TypeError, ValueError),
        )
        await checkpoints.record_failed_page(run_id, DOMAIN, offset, message, retryable=retryable)
        return message, retryable


async def _source_file_is_current(
    checkpoints: CheckpointStore,
    vendor: HarvardVendorConfig,
    remote_path: str,
    metadata: RemoteFileMetadata | None,
) -> bool:
    if metadata is None:
        return await checkpoints.source_file_completed(vendor.vendor, DOMAIN, remote_path)
    return await checkpoints.source_file_unchanged(
        vendor.vendor,
        DOMAIN,
        remote_path,
        metadata.size,
        metadata.modified_at,
    )


def _history_start_date(
    settings: Settings, vendor: HarvardVendorConfig, last_report_date: date
) -> date:
    raw_value = (
        settings.harvard_hmm_history_start_date
        if vendor.vendor == "harvard_hmm"
        else settings.harvard_spark_history_start_date
    ).strip()
    if not raw_value:
        return last_report_date
    try:
        first_report_date = date.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(f"{vendor.vendor.upper()}_HISTORY_START_DATE must use YYYY-MM-DD") from exc
    if first_report_date > last_report_date:
        raise ValueError(
            f"{vendor.vendor.upper()}_HISTORY_START_DATE must not be after "
            f"{last_report_date.isoformat()}"
        )
    return first_report_date


async def _fetch_report(
    settings: Settings,
    transport: SFTPTransport,
    remote_path: str,
    file_name: str,
    *,
    poll: bool,
    now: Callable[[], datetime],
    sleep: Callable[[float], Awaitable[None]],
) -> RemoteFile:
    current = now()
    if current.tzinfo is None:
        raise ValueError("Harvard SFTP clock must be timezone-aware")
    deadline = datetime.combine(current.date(), time(hour=7), current.tzinfo)
    seconds_until_deadline = max(0.0, (deadline - current).total_seconds())
    max_wait = (
        min(float(settings.harvard_sftp_max_wait_seconds), seconds_until_deadline) if poll else 0.0
    )
    elapsed = 0.0
    while True:
        remote_file = await transport.fetch(remote_path)
        if remote_file is not None:
            if remote_file.file_name != file_name or remote_file.remote_path != remote_path:
                raise ValueError("Harvard SFTP transport returned an unexpected file")
            return remote_file
        if elapsed >= max_wait:
            raise FileNotFoundError(f"Harvard report file was not available: {file_name}")
        wait_seconds = min(float(settings.harvard_sftp_poll_interval_seconds), max_wait - elapsed)
        await sleep(wait_seconds)
        elapsed += wait_seconds
