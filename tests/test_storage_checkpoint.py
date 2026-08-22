from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.checkpoint import CheckpointStore, JobAlreadyRunning, JobLockLost
from app.models import PageWrite, RunStatus
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


@pytest.mark.asyncio
async def test_vendor_lock_prevents_two_levelup_jobs(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state.db")
    await store.initialize()
    await store.acquire_lock("levelup", "run-1")
    with pytest.raises(JobAlreadyRunning):
        await store.acquire_lock("levelup", "run-2")
    await store.release_lock("levelup", "run-1")
    await store.acquire_lock("levelup", "run-2")
    await store.release_lock("levelup", "run-2")


@pytest.mark.asyncio
async def test_stale_vendor_lock_is_reclaimed_and_old_run_is_failed(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "state.db"
    store = CheckpointStore(database_path)
    await store.initialize()
    await store.start_run("run-1", "levelup")
    await store.acquire_lock("levelup", "run-1", ttl_seconds=60)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE vendor_locks SET heartbeat_at = ? WHERE vendor = 'levelup'",
            ("2000-01-01T00:00:00+00:00",),
        )

    await store.acquire_lock("levelup", "run-2", ttl_seconds=60)

    stale_run = await store.get_run("run-1")
    assert stale_run is not None
    assert stale_run.status.value == "failed"
    assert stale_run.error_message == "Stale vendor lock was reclaimed"
    with sqlite3.connect(database_path) as connection:
        lock_owner = connection.execute(
            "SELECT run_id FROM vendor_locks WHERE vendor = 'levelup'"
        ).fetchone()
    assert lock_owner == ("run-2",)
    await store.release_lock("levelup", "run-2")


@pytest.mark.asyncio
async def test_running_run_without_live_lock_is_resumable(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state.db")
    await store.initialize()
    await store.start_run("run-1", "levelup")

    assert await store.find_resumable_run("levelup", 60) == "run-1"


@pytest.mark.asyncio
async def test_resume_attempt_and_age_limits_stop_old_run_reuse(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    store = CheckpointStore(database_path)
    await store.initialize()
    await store.start_run("run-1", "levelup")
    await store.finish_run("run-1", RunStatus.FAILED, resume_eligible=True)

    assert await store.find_resumable_run("levelup", 60, 1, 24) == "run-1"

    await store.start_run("run-1", "levelup")
    await store.finish_run("run-1", RunStatus.FAILED, resume_eligible=True)
    assert await store.find_resumable_run("levelup", 60, 1, 24) is None

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE runs SET resume_attempts = 0, started_at = '2000-01-01T00:00:00+00:00' "
            "WHERE run_id = 'run-1'"
        )
    assert await store.find_resumable_run("levelup", 60, 1, 24) is None


@pytest.mark.asyncio
async def test_initialize_migrates_legacy_run_resume_columns(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                vendor TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT,
                catalog_completed INTEGER NOT NULL DEFAULT 0
            )
            """
        )

    store = CheckpointStore(database_path)
    await store.initialize()

    with sqlite3.connect(database_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    assert {"resume_eligible", "resume_attempts"} <= columns


@pytest.mark.asyncio
async def test_initialize_migrates_legacy_lock_table(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE vendor_locks (
                vendor TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO vendor_locks VALUES ('levelup', 'run-1', ?)",
            ("2026-08-21T00:00:00+00:00",),
        )

    store = CheckpointStore(database_path)
    await store.initialize()

    with sqlite3.connect(database_path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(vendor_locks)")
        }
        heartbeat = connection.execute(
            "SELECT heartbeat_at FROM vendor_locks WHERE vendor = 'levelup'"
        ).fetchone()
    assert "heartbeat_at" in columns
    assert heartbeat == ("2026-08-21T00:00:00+00:00",)


@pytest.mark.asyncio
async def test_heartbeat_renews_lock_and_detects_lost_ownership(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    store = CheckpointStore(database_path)
    await store.initialize()
    await store.acquire_lock("levelup", "run-1", ttl_seconds=60)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE vendor_locks SET heartbeat_at = '2000-01-01T00:00:00+00:00'"
        )

    await store.heartbeat_lock("levelup", "run-1")

    with pytest.raises(JobAlreadyRunning):
        await store.acquire_lock("levelup", "run-2", ttl_seconds=60)
    await store.release_lock("levelup", "run-1")
    with pytest.raises(JobLockLost):
        await store.heartbeat_lock("levelup", "run-1")


@pytest.mark.asyncio
async def test_purge_old_runs_keeps_live_and_latest_resumable_runs(tmp_path: Path) -> None:
    database_path = tmp_path / "state.db"
    store = CheckpointStore(database_path)
    await store.initialize()
    for run_id, status in (
        ("old-success", RunStatus.SUCCEEDED),
        ("old-failed", RunStatus.FAILED),
        ("latest-failed", RunStatus.PARTIAL_FAILURE),
    ):
        await store.start_run(run_id, "levelup")
        await store.record_completed_page(run_id, "course_catalog", 0, 1)
        await store.add_courses(run_id, ["course-1"])
        await store.finish_run(
            run_id,
            status,
            resume_eligible=status in {RunStatus.FAILED, RunStatus.PARTIAL_FAILURE},
        )
    await store.start_run("live-run", "levelup")
    await store.acquire_lock("levelup", "live-run")
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE runs SET started_at = '2000-01-01T00:00:00+00:00', "
            "finished_at = '2000-01-01T01:00:00+00:00' WHERE run_id = 'old-success'"
        )
        connection.execute(
            "UPDATE runs SET started_at = '2001-01-01T00:00:00+00:00', "
            "finished_at = '2001-01-01T01:00:00+00:00' WHERE run_id = 'old-failed'"
        )
        connection.execute(
            "UPDATE runs SET started_at = '2002-01-01T00:00:00+00:00', "
            "finished_at = '2002-01-01T01:00:00+00:00' WHERE run_id = 'latest-failed'"
        )

    assert await store.purge_old_runs("levelup", retention_days=30) == 2
    assert await store.get_run("old-success") is None
    assert await store.get_run("old-failed") is None
    assert await store.get_run("latest-failed") is not None
    assert await store.get_run("live-run") is not None
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM checkpoints WHERE run_id IN ('old-success', 'old-failed')"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM run_courses WHERE run_id IN ('old-success', 'old-failed')"
        ).fetchone() == (0,)
    await store.release_lock("levelup", "live-run")
