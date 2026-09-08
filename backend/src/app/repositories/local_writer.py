from __future__ import annotations

import asyncio
import base64
import csv
import io
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import quote

from app.models import BinaryFileWrite, PageWrite
from app.repositories.writer import (
    StorageWriteResult,
    merge_file_manifest,
    merge_page_manifest,
    payload_sha256,
)

logger = logging.getLogger(__name__)

_IS_WINDOWS = os.name == "nt"
_MANIFEST_FILENAME = "manifest.json"
_WRITE_RETRY_DELAYS = (0.05, 0.10, 0.20, 0.40, 0.80, 1.60, 3.20)
_JSON_RECORD_KEYS = (
    "courses",
    "enrollments",
    "items",
    "list",
    "reports",
    "elements",
    "classList",
    "studentList",
)


class LocalBronzeWriter:
    """Local filesystem Bronze implementation; it is not a OneLake emulator."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._manifest_locks_guard = Lock()
        self._manifest_locks: dict[Path, Lock] = {}

    async def write_page(self, page: PageWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_page, page)

    async def write_file(self, file: BinaryFileWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_file, file)

    async def export_csv(self, vendors: list[str]) -> str:
        return await asyncio.to_thread(self._export_csv, vendors)

    async def clear_vendors(self, vendors: list[str]) -> tuple[int, int]:
        return await asyncio.to_thread(self._clear_vendors, vendors)

    def _write_page(self, page: PageWrite) -> StorageWriteResult:
        if not page.raw_payload:
            raise ValueError("Refusing to write an empty raw payload")

        directory = (
            self.root
            / page.vendor
            / page.data_domain
            / f"ingestion_date={page.ingestion_date}"
            / f"run_id={quote(page.run_id, safe='-_')}"
        )
        if page.course_id is not None:
            directory /= f"course_id={quote(page.course_id, safe='-_.')}"
        directory.mkdir(parents=True, exist_ok=True)

        output_path = directory / f"offset={page.offset:06d}.json"
        self._atomic_write(output_path, page.raw_payload)

        manifest_path = directory / _MANIFEST_FILENAME
        sha256 = payload_sha256(page.raw_payload)
        with self._manifest_lock_for(manifest_path):
            manifest = merge_page_manifest(
                self._load_manifest(manifest_path),
                page,
                file_name=output_path.name,
                sha256=sha256,
            )
            self._write_manifest(
                manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            )

        logger.debug(
            "Bronze page stored vendor=%s domain=%s run_id=%s offset=%d "
            "records_count=%d payload_bytes=%d file=%s",
            page.vendor,
            page.data_domain,
            page.run_id,
            page.offset,
            page.records_count,
            len(page.raw_payload),
            output_path.name,
        )
        return StorageWriteResult(
            uri=output_path.resolve().as_uri(),
            size_bytes=len(page.raw_payload),
            sha256=sha256,
        )

    def _write_file(self, file: BinaryFileWrite) -> StorageWriteResult:
        if not file.raw_payload:
            raise ValueError("Refusing to write an empty raw payload")
        if Path(file.file_name).name != file.file_name:
            raise ValueError("Binary Bronze file_name must not contain a path")
        if file.file_size != len(file.raw_payload):
            raise ValueError("Binary Bronze file size does not match payload")

        directory = (
            self.root
            / file.vendor
            / file.data_domain
            / f"ingestion_date={file.ingestion_date}"
            / f"run_id={quote(file.run_id, safe='-_')}"
        )
        directory.mkdir(parents=True, exist_ok=True)

        output_path = directory / file.file_name
        self._atomic_write(output_path, file.raw_payload)

        manifest_path = directory / _MANIFEST_FILENAME
        sha256 = payload_sha256(file.raw_payload)
        with self._manifest_lock_for(manifest_path):
            manifest = merge_file_manifest(
                self._load_manifest(manifest_path),
                file,
                sha256=sha256,
            )
            self._write_manifest(
                manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            )

        logger.debug(
            "Bronze file stored vendor=%s domain=%s run_id=%s "
            "records_count=%d payload_bytes=%d file=%s",
            file.vendor,
            file.data_domain,
            file.run_id,
            file.records_count,
            len(file.raw_payload),
            output_path.name,
        )
        return StorageWriteResult(
            uri=output_path.resolve().as_uri(),
            size_bytes=len(file.raw_payload),
            sha256=sha256,
        )

    def _export_csv(self, vendors: list[str]) -> str:
        output = io.StringIO(newline="")
        fieldnames = [
            "vendor",
            "data_domain",
            "ingestion_date",
            "run_id",
            "source_file",
            "record_index",
            "raw_record_json",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        root = self.root.resolve()
        for vendor in vendors:
            vendor_root = (root / vendor).resolve()
            if vendor_root.parent != root or not vendor_root.exists():
                continue
            for path in sorted(item for item in vendor_root.rglob("*") if item.is_file()):
                if path.name == _MANIFEST_FILENAME or path.name.startswith("."):
                    continue
                metadata = self._path_metadata(root, path)
                for index, raw_record in enumerate(self._records_for_export(path)):
                    writer.writerow(
                        {
                            **metadata,
                            "record_index": index,
                            "raw_record_json": json.dumps(
                                raw_record,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        }
                    )
        return output.getvalue()

    def _clear_vendors(self, vendors: list[str]) -> tuple[int, int]:
        root = self.root.resolve()
        objects_deleted = 0
        bytes_deleted = 0
        for vendor in vendors:
            target = (root / vendor).resolve()
            if target.parent != root:
                raise ValueError("Cleanup target escaped the configured Bronze root")
            if not target.exists():
                continue
            files = [item for item in target.rglob("*") if item.is_file()]
            objects_deleted += len(files)
            bytes_deleted += sum(item.stat().st_size for item in files)
            shutil.rmtree(target)
        return objects_deleted, bytes_deleted

    @staticmethod
    def _path_metadata(root: Path, path: Path) -> dict[str, str]:
        relative = path.relative_to(root)
        parts = relative.parts
        return {
            "vendor": parts[0] if parts else "",
            "data_domain": parts[1] if len(parts) > 1 else "",
            "ingestion_date": next(
                (
                    part.removeprefix("ingestion_date=")
                    for part in parts
                    if part.startswith("ingestion_date=")
                ),
                "",
            ),
            "run_id": next(
                (
                    part.removeprefix("run_id=")
                    for part in parts
                    if part.startswith("run_id=")
                ),
                "",
            ),
            "source_file": relative.as_posix(),
        }

    @staticmethod
    def _records_for_export(path: Path) -> list[object]:
        payload = path.read_bytes()
        if path.suffix.casefold() == ".json":
            try:
                return LocalBronzeWriter._json_records(json.loads(payload))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        if path.suffix.casefold() == ".csv":
            try:
                records: list[object] = list(
                    csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
                )
                return records or [{"raw_text": payload.decode("utf-8-sig")}]
            except UnicodeDecodeError:
                pass
        return [{"raw_base64": base64.b64encode(payload).decode("ascii")}]

    @staticmethod
    def _json_records(payload: object) -> list[object]:
        if isinstance(payload, list):
            return list(payload)
        if not isinstance(payload, dict):
            return [payload]

        containers = [payload]
        nested_data = payload.get("data")
        if isinstance(nested_data, list):
            return list(nested_data)
        if isinstance(nested_data, dict):
            containers.append(nested_data)

        records: list[object] = []
        recognized_container = False
        for container in containers:
            for key in _JSON_RECORD_KEYS:
                value = container.get(key)
                if isinstance(value, list):
                    recognized_container = True
                    records.extend(value)
        return records if recognized_container else [payload]

    def _manifest_lock_for(self, path: Path) -> Lock:
        key = path.resolve()
        with self._manifest_locks_guard:
            lock = self._manifest_locks.get(key)
            if lock is None:
                lock = Lock()
                self._manifest_locks[key] = lock
            return lock

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return (
            {str(key): item for key, item in value.items()}
            if isinstance(value, dict)
            else {}
        )

    @staticmethod
    def _write_manifest(path: Path, payload: bytes) -> None:
        if not _IS_WINDOWS:
            LocalBronzeWriter._atomic_write(path, payload)
            return

        last_error: PermissionError | None = None
        for attempt in range(len(_WRITE_RETRY_DELAYS) + 1):
            try:
                # Windows endpoint protection can open an existing manifest without
                # delete sharing, which blocks os.replace(). The caller holds the
                # per-manifest lock, so an in-place local-development write is safe
                # from competing threads in this backend process.
                with path.open("wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())

                if attempt:
                    logger.debug(
                        "Manifest write succeeded after temporary Windows lock "
                        "file=%s attempts=%d",
                        path,
                        attempt + 1,
                    )
                return
            except PermissionError as exc:
                last_error = exc
                if attempt >= len(_WRITE_RETRY_DELAYS):
                    break
                time.sleep(_WRITE_RETRY_DELAYS[attempt])

        logger.error(
            "Manifest remained locked after retries file=%s attempts=%d",
            path,
            len(_WRITE_RETRY_DELAYS) + 1,
        )
        assert last_error is not None
        raise last_error

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            dir=path.parent,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())

            last_error: PermissionError | None = None
            for attempt in range(len(_WRITE_RETRY_DELAYS) + 1):
                try:
                    os.replace(temporary_name, path)
                    return
                except PermissionError as exc:
                    last_error = exc
                    if attempt >= len(_WRITE_RETRY_DELAYS):
                        break
                    time.sleep(_WRITE_RETRY_DELAYS[attempt])

            assert last_error is not None
            raise last_error
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
