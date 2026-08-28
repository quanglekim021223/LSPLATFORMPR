from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol

from app.core.security import sanitize_mapping
from app.models import BinaryFileWrite, PageWrite


@dataclass(frozen=True, slots=True)
class StorageWriteResult:
    uri: str
    size_bytes: int
    sha256: str


class BronzeWriter(Protocol):
    async def write_page(self, page: PageWrite) -> StorageWriteResult: ...

    async def write_file(self, file: BinaryFileWrite) -> StorageWriteResult: ...


def payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def merge_page_manifest(
    current: dict[str, Any],
    page: PageWrite,
    *,
    file_name: str,
    sha256: str,
) -> dict[str, Any]:
    manifest = dict(current)
    manifest.update(
        {
            "vendor": page.vendor,
            "data_domain": page.data_domain,
            "ingestion_date": page.ingestion_date,
            "run_id": page.run_id,
            "course_id": page.course_id,
        }
    )
    raw_pages = manifest.get("pages")
    pages = (
        {
            int(item["offset"]): item
            for item in raw_pages
            if isinstance(item, dict) and "offset" in item
        }
        if isinstance(raw_pages, list)
        else {}
    )
    pages[page.offset] = {
        "offset": page.offset,
        "file": file_name,
        "records_count": page.records_count,
        "request_parameters": sanitize_mapping(page.request_parameters),
        "fetched_at": page.fetched_at.isoformat(),
        "sha256": sha256,
    }
    manifest["pages"] = [pages[offset] for offset in sorted(pages)]
    manifest["records_count"] = sum(
        int(item.get("records_count", 0)) for item in pages.values()
    )
    manifest["updated_at"] = page.fetched_at.isoformat()
    return manifest


def merge_file_manifest(
    current: dict[str, Any],
    file: BinaryFileWrite,
    *,
    sha256: str,
) -> dict[str, Any]:
    manifest = dict(current)
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
        "sha256": sha256,
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
    return manifest
