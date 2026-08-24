from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote

from app.helpers.security import sanitize_mapping
from app.models import BinaryFileWrite, PageWrite

logger = logging.getLogger(__name__)


class LocalBronzeWriter:
    """Local filesystem Bronze implementation; it is not a OneLake emulator."""

    def __init__(self, root: Path) -> None:
        self.root = root

    async def write_page(self, page: PageWrite) -> Path:
        return await asyncio.to_thread(self._write_page, page)

    async def write_file(self, file: BinaryFileWrite) -> Path:
        return await asyncio.to_thread(self._write_file, file)

    def _write_page(self, page: PageWrite) -> Path:
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
        manifest = self._load_manifest(manifest_path)
        manifest.update(
            {
                "vendor": page.vendor,
                "data_domain": page.data_domain,
                "ingestion_date": page.ingestion_date,
                "run_id": page.run_id,
                "course_id": page.course_id,
            }
        )
        pages: dict[int, dict[str, Any]] = {}
        raw_pages = manifest.get("pages")
        if isinstance(raw_pages, list):
            pages = {
                int(item["offset"]): item
                for item in raw_pages
                if isinstance(item, dict) and "offset" in item
            }
        pages[page.offset] = {
            "offset": page.offset,
            "file": output_path.name,
            "records_count": page.records_count,
            "request_parameters": sanitize_mapping(page.request_parameters),
            "fetched_at": page.fetched_at.isoformat(),
            "sha256": hashlib.sha256(page.raw_payload).hexdigest(),
        }
        manifest["pages"] = [pages[offset] for offset in sorted(pages)]
        manifest["records_count"] = sum(
            int(item.get("records_count", 0)) for item in pages.values()
        )
        manifest["updated_at"] = page.fetched_at.isoformat()
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
        return output_path

    def _write_file(self, file: BinaryFileWrite) -> Path:
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
        manifest = self._load_manifest(manifest_path)
        manifest.update(
            {
                "vendor": file.vendor,
                "data_domain": file.data_domain,
                "ingestion_date": file.ingestion_date,
                "run_id": file.run_id,
            }
        )
        entry = {
            "file": file.file_name,
            "remote_filename": file.file_name,
            "remote_path": file.remote_path,
            "file_size": file.file_size,
            "remote_modified_time": file.remote_modified_time.isoformat(),
            "downloaded_at": file.downloaded_at.isoformat(),
            "sha256": hashlib.sha256(file.raw_payload).hexdigest(),
            "records_count": file.records_count,
        }
        files = {
            str(item["remote_path"]): item
            for item in manifest.get("files", [])
            if isinstance(item, dict) and "remote_path" in item
        }
        files[file.remote_path] = entry
        manifest["files"] = [files[path] for path in sorted(files)]
        manifest.update(entry)
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
        return output_path

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
