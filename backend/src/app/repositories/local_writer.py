from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
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


class LocalBronzeWriter:
    """Local filesystem Bronze implementation; it is not a OneLake emulator."""

    def __init__(self, root: Path) -> None:
        self.root = root

    async def write_page(self, page: PageWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_page, page)

    async def write_file(self, file: BinaryFileWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_file, file)

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

        manifest_path = directory / "manifest.json"
        sha256 = payload_sha256(page.raw_payload)
        manifest = merge_page_manifest(
            self._load_manifest(manifest_path),
            page,
            file_name=output_path.name,
            sha256=sha256,
        )
        self._atomic_write(
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
        manifest_path = directory / "manifest.json"
        sha256 = payload_sha256(file.raw_payload)
        manifest = merge_file_manifest(
            self._load_manifest(manifest_path),
            file,
            sha256=sha256,
        )
        self._atomic_write(
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

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise
