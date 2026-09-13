from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from app.models import BinaryFileWrite, PageWrite
from app.repositories import LocalBronzeWriter


def _exported_records(payload: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(payload)))


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"items": [{"id": "skillup"}]}, [{"id": "skillup"}]),
        ({"reports": [{"id": "assessment"}]}, [{"id": "assessment"}]),
        ({"elements": [{"id": "coursera"}]}, [{"id": "coursera"}]),
        ({"elements": []}, []),
        ({"data": [{"id": "datacamp"}]}, [{"id": "datacamp"}]),
        ([{"id": "root-list"}], [{"id": "root-list"}]),
    ],
)
def test_json_export_recognizes_supported_record_containers(
    payload: object,
    expected: list[object],
) -> None:
    assert LocalBronzeWriter._json_records(payload) == expected


@pytest.mark.asyncio
async def test_local_bronze_writer_preserves_bytes_and_sanitizes_manifest(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = b'{"enrollments":[{"id":"e1"}],"spacing": true}\n'
    writer = LocalBronzeWriter(tmp_path / "bronze")
    caplog.set_level(logging.DEBUG, logger="app.repositories.local_writer")
    result = await writer.write_page(
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
    path = Path(unquote(urlparse(result.uri).path))
    assert await asyncio.to_thread(path.read_bytes) == raw
    assert result.size_bytes == len(raw)
    assert result.sha256 == hashlib.sha256(raw).hexdigest()
    assert "course%2Funsafe" in str(path)
    manifest = json.loads((path.parent / "manifest.json").read_text())
    assert manifest["records_count"] == 1
    assert manifest["pages"][0]["request_parameters"]["Authorization"] == "[REDACTED]"
    assert "secret-token" not in (path.parent / "manifest.json").read_text()
    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "app.repositories.local_writer"
    )
    assert "Bronze page stored vendor=levelup domain=learning_history" in messages
    assert "offset=0" in messages
    assert "records_count=1" in messages
    assert "payload_bytes=" in messages
    assert "secret-token" not in messages


@pytest.mark.asyncio
async def test_writer_rejects_empty_payload(tmp_path: Path) -> None:
    writer = LocalBronzeWriter(tmp_path)
    write = PageWrite(
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
    with pytest.raises(ValueError, match="empty"):
        await writer.write_page(write)
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
    caplog.set_level(logging.DEBUG, logger="app.repositories.local_writer")
    result = await writer.write_file(
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
    path = Path(unquote(urlparse(result.uri).path))
    assert await asyncio.to_thread(path.read_bytes) == raw
    assert result.size_bytes == len(raw)
    assert result.sha256 == hashlib.sha256(raw).hexdigest()
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
        if record.name == "app.repositories.local_writer"
    )
    assert "Bronze file stored vendor=harvard_hmm domain=learning_history" in messages
    assert "records_count=1" in messages
    assert "payload_bytes=" in messages


@pytest.mark.asyncio
async def test_csv_export_flattens_json_record_collections(tmp_path: Path) -> None:
    writer = LocalBronzeWriter(tmp_path / "bronze")
    fetched_at = datetime.now(UTC)

    await writer.write_page(
        PageWrite(
            vendor="levelup",
            data_domain="course_catalog",
            ingestion_date="2026-09-07",
            run_id="levelup-run",
            offset=0,
            raw_payload=b'{"courses":[{"id":"c1"},{"id":"c2"}],"totalItems":2}',
            records_count=2,
            request_parameters={},
            fetched_at=fetched_at,
        )
    )
    await writer.write_page(
        PageWrite(
            vendor="fams",
            data_domain="training_data",
            ingestion_date="2026-09-07",
            run_id="fams-run",
            offset=1,
            raw_payload=(
                b'{"success":true,"data":{"classList":[{"id":"class-1"}],'
                b'"studentList":[{"id":"student-1"},{"id":"student-2"}]}}'
            ),
            records_count=3,
            request_parameters={},
            fetched_at=fetched_at,
        )
    )
    await writer.write_page(
        PageWrite(
            vendor="harvard_hmm",
            data_domain="course_catalog",
            ingestion_date="2026-09-07",
            run_id="harvard-run",
            offset=0,
            raw_payload=b'{"count":2,"list":[{"id":"h1"},{"id":"h2"}]}',
            records_count=2,
            request_parameters={},
            fetched_at=fetched_at,
        )
    )
    history = b"username,product_id\nuser-1,p1\nuser-2,p2\n"
    await writer.write_file(
        BinaryFileWrite(
            vendor="harvard_hmm",
            data_domain="learning_history",
            ingestion_date="2026-09-07",
            run_id="harvard-run",
            raw_payload=history,
            file_name="harvard_hmm_reporting_20260907.csv",
            remote_path="/reports/harvard_hmm_reporting_20260907.csv",
            file_size=len(history),
            remote_modified_time=fetched_at,
            downloaded_at=fetched_at,
            records_count=2,
        )
    )

    levelup_rows = _exported_records(await writer.export_csv(["levelup"]))
    fams_rows = _exported_records(await writer.export_csv(["fams"]))
    harvard_rows = _exported_records(await writer.export_csv(["harvard_hmm"]))

    assert [json.loads(row["raw_record_json"])["id"] for row in levelup_rows] == [
        "c1",
        "c2",
    ]
    assert [json.loads(row["raw_record_json"])["id"] for row in fams_rows] == [
        "class-1",
        "student-1",
        "student-2",
    ]
    assert len(harvard_rows) == 4
    assert [json.loads(row["raw_record_json"]).get("id") for row in harvard_rows[:2]] == [
        "h1",
        "h2",
    ]
    assert [json.loads(row["raw_record_json"])["username"] for row in harvard_rows[2:]] == [
        "user-1",
        "user-2",
    ]
