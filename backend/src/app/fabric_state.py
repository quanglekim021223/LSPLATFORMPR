"""A leased Blob holds the committed checkpoint or an immutable publish batch."""

from __future__ import annotations

import shutil
import threading
from pathlib import Path
from types import TracebackType
from zipfile import ZIP_DEFLATED, ZipFile

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobClient, BlobLeaseClient

CHECKPOINT_FILE = "checkpoint.db"
STATE_FILE = "state.json"
TABLES_DIRECTORY = "tables"


def pack_state(directory: Path, output: Path, *, pending: bool) -> None:
    paths = [directory / CHECKPOINT_FILE, directory / STATE_FILE]
    if pending:
        paths.extend(sorted((directory / TABLES_DIRECTORY).glob("*/*.parquet")))
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, path.relative_to(directory).as_posix())


def _valid_archive_member(filename: str) -> bool:
    path = Path(filename)
    return filename in {CHECKPOINT_FILE, STATE_FILE} or (
        len(path.parts) == 3
        and path.parts[0] == TABLES_DIRECTORY
        and path.suffix == ".parquet"
        and ".." not in path.parts
    )


def unpack_state(archive_path: Path, destination: Path) -> None:
    with ZipFile(archive_path) as archive:
        seen: set[str] = set()
        for item in archive.infolist():
            path = Path(item.filename)
            if not _valid_archive_member(item.filename) or item.filename in seen or item.is_dir():
                raise ValueError("Invalid checkpoint archive member")
            seen.add(item.filename)
            target = destination / path
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(item) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
        if not {STATE_FILE, CHECKPOINT_FILE} <= seen:
            raise ValueError("Incomplete checkpoint archive")


class BlobState:
    """One renewable 60-second lease fences all durable state changes."""

    def __init__(self, blob: BlobClient) -> None:
        self.blob = blob
        self.lease: BlobLeaseClient | None = None
        self.stop = threading.Event()
        self.lost = threading.Event()
        self.thread: threading.Thread | None = None

    def __enter__(self) -> BlobState:
        try:
            self.blob.upload_blob(b"", overwrite=False)
        except ResourceExistsError:
            pass
        self.lease = self.blob.acquire_lease(lease_duration=60)
        self.thread = threading.Thread(target=self._renew, daemon=True)
        self.thread.start()
        return self

    def _renew(self) -> None:
        while not self.stop.wait(15):
            lease = self.lease
            if lease is None:
                self.lost.set()
                return
            try:
                lease.renew(timeout=15)
            except Exception:
                self.lost.set()
                return

    def check(self) -> None:
        if self.lost.is_set() or self.lease is None:
            raise RuntimeError("Fabric checkpoint lease was lost")

    def download(self, destination: Path) -> bool:
        self.check()
        with destination.open("wb") as output:
            self.blob.download_blob(lease=self.lease).readinto(output)
        return destination.stat().st_size > 0

    def upload(self, source: Path) -> None:
        self.check()
        with source.open("rb") as stream:
            self.blob.upload_blob(stream, overwrite=True, lease=self.lease)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=30)
        if self.lease is not None and not self.lost.is_set():
            self.lease.release()
