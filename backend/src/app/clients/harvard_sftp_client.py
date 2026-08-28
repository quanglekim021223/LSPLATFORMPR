from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, Self

from app.core.config import Settings
from app.core.retry import backoff_seconds
from app.models.harvard import RemoteFile, RemoteFileMetadata


class AsyncSSHSFTPTransport:
    def __init__(
        self,
        settings: Settings,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        if settings.harvard_sftp_known_hosts is None:
            raise ValueError(
                "HARVARD_SFTP_KNOWN_HOSTS is required for host-key verification"
            )
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

    async def list_files(self, remote_dir: str) -> list[RemoteFileMetadata]:
        close_after_list = self._sftp is None
        try:
            for retry_number in range(self.settings.harvard_sftp_max_retries + 1):
                try:
                    await self._open_session()
                    return await self._list_from_open_session(remote_dir)
                except Exception as exc:
                    await self._close_session()
                    if (
                        retry_number >= self.settings.harvard_sftp_max_retries
                        or not is_retryable_sftp_error(exc)
                    ):
                        raise
                    await self._sleep(backoff_seconds(retry_number, self._jitter))
        finally:
            if close_after_list:
                await self._close_session()
        raise AssertionError("Harvard SFTP list retry loop exhausted")

    async def _list_from_open_session(
        self, remote_dir: str
    ) -> list[RemoteFileMetadata]:
        entries = await self._sftp.readdir(remote_dir)
        files: list[RemoteFileMetadata] = []
        for entry in entries:
            file_name = str(entry.filename)
            if file_name in {".", ".."}:
                continue
            remote_path = str(PurePosixPath(remote_dir) / file_name)
            attributes = entry.attrs
            if attributes.size is None or attributes.mtime is None:
                attributes = await self._sftp.stat(remote_path)
            if attributes.size is None or attributes.mtime is None:
                raise ValueError("Harvard SFTP file metadata is incomplete")
            files.append(
                RemoteFileMetadata(
                    remote_path=remote_path,
                    file_name=file_name,
                    size=int(attributes.size),
                    modified_at=datetime.fromtimestamp(int(attributes.mtime), UTC),
                )
            )
        return files

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
