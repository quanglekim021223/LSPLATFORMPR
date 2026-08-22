from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.models import PageWrite
from app.storage import LocalBronzeWriter


@pytest.mark.asyncio
async def test_local_bronze_writer_preserves_bytes_and_sanitizes_manifest(
    tmp_path: Path,
) -> None:
    raw = b'{"enrollments":[{"id":"e1"}],"spacing": true}\n'
    writer = LocalBronzeWriter(tmp_path / "bronze")
    path = await writer.write_page(
        PageWrite(
            vendor="levelup",
            data_domain="learning_history",
            ingestion_date="2026-08-21",
            run_id="11111111-1111-4111-8111-111111111111",
            course_id="course/unsafe",
            offset=0,
            raw_payload=raw,
            records_count=1,
            request_parameters={"_offset": 0, "Authorization": "secret-token"},
            fetched_at=datetime.now(UTC),
        )
    )
    assert path.read_bytes() == raw
    assert "course%2Funsafe" in str(path)
    manifest = json.loads((path.parent / "manifest.json").read_text())
    assert manifest["records_count"] == 1
    assert manifest["pages"][0]["request_parameters"]["Authorization"] == "[REDACTED]"
    assert "secret-token" not in (path.parent / "manifest.json").read_text()


@pytest.mark.asyncio
async def test_writer_rejects_empty_payload(tmp_path: Path) -> None:
    writer = LocalBronzeWriter(tmp_path)
    with pytest.raises(ValueError, match="empty"):
        await writer.write_page(
            PageWrite(
                vendor="levelup",
                data_domain="course_catalog",
                ingestion_date="2026-08-21",
                run_id="11111111-1111-4111-8111-111111111111",
                offset=0,
                raw_payload=b"",
                records_count=0,
                request_parameters={},
                fetched_at=datetime.now(UTC),
            )
        )
    json_files = await asyncio.to_thread(lambda: list(tmp_path.rglob("*.json")))
    assert json_files == []
