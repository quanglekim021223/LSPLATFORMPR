from __future__ import annotations

import asyncio
import posixpath
import random
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from datetime import UTC, date, datetime, time, timedelta
from pathlib import PurePosixPath
from typing import Any, Self

from app.config import Settings
from app.helpers.retry import backoff_seconds
from app.helpers.security import sanitize_text
from app.models import BinaryFileWrite
from app.repositories.checkpoint_repository import CheckpointStore
from app.storage import BronzeWriter
from app.vendors.harvard.models import HarvardVendorConfig, RemoteFile, SFTPTransport

DOMAIN = "learning_history"


class HarvardHistoryIngestionError(RuntimeError):
    pass


class AsyncSSHSFTPTransport:
    def __init__(
        self,
        settings: Settings,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        if settings.harvard_sftp_known_hosts is None:
            raise ValueError("HARVARD_SFTP_KNOWN_HOSTS is required for host-key verification")
        self.settings = settings
        self._stack: AsyncExitStack | None = None
        self._sftp: Any = None
        self._asyncssh: Any = None
        self._sleep = sleep
        self._jitter = jitter

    async def __aenter__(self) -> Self:
        for retry_number in range(self.settings.harvard_sftp_max_retries + 1):
            try:
                await self._open_session()
                return self
            except Exception as exc:
                if (
                    retry_number >= self.settings.harvard_sftp_max_retries
                    or not is_retryable_sftp_error(exc)
                ):
                    raise
                await self._sleep(backoff_seconds(retry_number, self._jitter))
        raise AssertionError("Harvard SFTP connection retry loop exhausted")

    async def _open_session(self) -> None:
        if self._sftp is not None:
            return
        import asyncssh

        self._asyncssh = asyncssh
        stack = AsyncExitStack()
        try:
            connection = await stack.enter_async_context(
                asyncssh.connect(
                    self.settings.harvard_sftp_host,
                    port=self.settings.harvard_sftp_port,
                    username=self.settings.harvard_sftp_username.get_secret_value(),
                    password=self.settings.harvard_sftp_password.get_secret_value(),
                    known_hosts=str(self.settings.harvard_sftp_known_hosts),
                )
            )
            self._sftp = await stack.enter_async_context(
                connection.start_sftp_client()
            )
        except BaseException:
            await stack.aclose()
            raise
        self._stack = stack

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object | None,
    ) -> None:
        await self._close_session()

    async def _close_session(self) -> None:
        if self._stack is not None:
            await self._stack.aclose()
        self._stack = None
        self._sftp = None
        self._asyncssh = None

    async def fetch(self, remote_path: str) -> RemoteFile | None:
        close_after_fetch = self._sftp is None
        try:
            for retry_number in range(self.settings.harvard_sftp_max_retries + 1):
                try:
                    await self._open_session()
                    return await self._fetch_from_open_session(remote_path)
                except Exception as exc:
                    await self._close_session()
                    if (
                        retry_number >= self.settings.harvard_sftp_max_retries
                        or not is_retryable_sftp_error(exc)
                    ):
                        raise
                    await self._sleep(backoff_seconds(retry_number, self._jitter))
        finally:
            if close_after_fetch:
                await self._close_session()
        raise AssertionError("Harvard SFTP operation retry loop exhausted")

    async def _fetch_from_open_session(self, remote_path: str) -> RemoteFile | None:
        try:
            attributes = await self._sftp.stat(remote_path)
        except self._asyncssh.SFTPNoSuchFile:
            return None
        if attributes.size is None or attributes.mtime is None:
            raise ValueError("Harvard SFTP file metadata is incomplete")
        async with self._sftp.open(remote_path, "rb") as remote_file:
            content = await remote_file.read()
        if not isinstance(content, bytes):
            raise TypeError("Harvard SFTP download did not return bytes")
        return RemoteFile(
            remote_path=remote_path,
            file_name=PurePosixPath(remote_path).name,
            content=content,
            size=int(attributes.size),
            modified_at=datetime.fromtimestamp(int(attributes.mtime), UTC),
        )


def is_retryable_sftp_error(exc: BaseException) -> bool:
    import asyncssh

    return isinstance(
        exc,
        (
            TimeoutError,
            ConnectionError,
            OSError,
            EOFError,
            asyncssh.ConnectionLost,
            asyncssh.SFTPConnectionLost,
        ),
    )


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
    last_report_date = current.date() - timedelta(
        days=settings.harvard_report_date_offset_days
    )
    try:
        first_report_date = _history_start_date(settings, vendor, last_report_date)
    except ValueError as exc:
        message = sanitize_text(exc, settings.harvard_secrets(vendor.vendor))
        await checkpoints.record_failed_page(
            run_id, DOMAIN, 0, message, retryable=False
        )
        await checkpoints.mark_domain(run_id, DOMAIN, "terminal_failed", message)
        raise

    failures: list[tuple[str, bool]] = []
    report_date = first_report_date
    while report_date <= last_report_date:
        file_name = f"{vendor.report_filename_prefix}{report_date:%Y%m%d}.csv"
        remote_path = posixpath.join(settings.harvard_sftp_remote_dir, file_name)
        source_key = remote_path
        offset = int(report_date.strftime("%Y%m%d"))
        if await checkpoints.source_file_completed(vendor.vendor, DOMAIN, source_key):
            report_date += timedelta(days=1)
            continue
        try:
            remote_file = await _fetch_report(
                settings,
                transport,
                remote_path,
                file_name,
                poll=report_date == last_report_date,
                now=now,
                sleep=sleep,
            )
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
                )
            )
            await checkpoints.record_completed_page(run_id, DOMAIN, offset, 0)
            await checkpoints.record_completed_source_file(
                vendor.vendor, DOMAIN, source_key, run_id
            )
        except Exception as exc:
            message = sanitize_text(exc, settings.harvard_secrets(vendor.vendor))
            retryable = not isinstance(exc, (FileNotFoundError, TypeError, ValueError))
            await checkpoints.record_failed_page(
                run_id, DOMAIN, offset, message, retryable=retryable
            )
            failures.append((message, retryable))
        report_date += timedelta(days=1)

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
        raise ValueError(
            f"{vendor.vendor.upper()}_HISTORY_START_DATE must use YYYY-MM-DD"
        ) from exc
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
        min(float(settings.harvard_sftp_max_wait_seconds), seconds_until_deadline)
        if poll
        else 0.0
    )
    elapsed = 0.0
    while True:
        remote_file = await transport.fetch(remote_path)
        if remote_file is not None:
            if (
                remote_file.file_name != file_name
                or remote_file.remote_path != remote_path
            ):
                raise ValueError("Harvard SFTP transport returned an unexpected file")
            return remote_file
        if elapsed >= max_wait:
            raise FileNotFoundError(f"Harvard report file was not available: {file_name}")
        wait_seconds = min(
            float(settings.harvard_sftp_poll_interval_seconds), max_wait - elapsed
        )
        await sleep(wait_seconds)
        elapsed += wait_seconds
