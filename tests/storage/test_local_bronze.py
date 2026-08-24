from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.models import BinaryFileWrite, PageWrite
from app.storage import LocalBronzeWriter


@pytest.mark.asyncio
async def test_local_bronze_writer_preserves_bytes_and_sanitizes_manifest(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = b'{"enrollments":[{"id":"e1"}],"spacing": true}\n'
    writer = LocalBronzeWriter(tmp_path / "bronze")
    caplog.set_level(logging.DEBUG, logger="app.storage.local_bronze")
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
    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "app.storage.local_bronze"
    )
    assert "Bronze page stored vendor=levelup domain=learning_history" in messages
    assert "offset=0" in messages
    assert "records_count=1" in messages
    assert "payload_bytes=" in messages
    assert "secret-token" not in messages


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


@pytest.mark.asyncio
async def test_binary_writer_preserves_csv_and_sftp_manifest(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = b"learner_id,course_id\n1,c1\n"
    downloaded_at = datetime.now(UTC)
    modified_at = datetime(2026, 8, 22, 3, 0, tzinfo=UTC)
    writer = LocalBronzeWriter(tmp_path / "bronze")
    caplog.set_level(logging.DEBUG, logger="app.storage.local_bronze")
    path = await writer.write_file(
        BinaryFileWrite(
            vendor="harvard_hmm",
            data_domain="learning_history",
            ingestion_date="2026-08-23",
            run_id="11111111-1111-4111-8111-111111111111",
            raw_payload=raw,
            file_name="harvard_hmm_reporting_20260822.csv",
            remote_path="/fpt_sparkprod_feed/harvard_hmm_reporting_20260822.csv",
            file_size=len(raw),
            remote_modified_time=modified_at,
            downloaded_at=downloaded_at,
            records_count=1,
        )
    )
    assert path.read_bytes() == raw
    manifest = json.loads((path.parent / "manifest.json").read_text())
    assert manifest["remote_filename"] == path.name
    assert manifest["remote_path"].endswith(path.name)
    assert manifest["file_size"] == len(raw)
    assert manifest["remote_modified_time"] == modified_at.isoformat()
    assert manifest["downloaded_at"] == downloaded_at.isoformat()
    assert manifest["sha256"] == hashlib.sha256(raw).hexdigest()
    assert manifest["records_count"] == 1
    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "app.storage.local_bronze"
    )
    assert "Bronze file stored vendor=harvard_hmm domain=learning_history" in messages
    assert "records_count=1" in messages
    assert "payload_bytes=" in messages
