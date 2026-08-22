from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.models import RunStatus, RunSummary


class JobAlreadyRunning(RuntimeError):
    pass


class JobLockLost(RuntimeError):
    pass


class CheckpointStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    async def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._initialize)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    vendor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    error_message TEXT,
                    catalog_completed INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS checkpoints (
                    run_id TEXT NOT NULL,
                    vendor TEXT NOT NULL,
                    data_domain TEXT NOT NULL,
                    course_id TEXT NOT NULL DEFAULT '',
                    offset INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    records_count INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    PRIMARY KEY (run_id, data_domain, course_id, offset)
                );
                CREATE TABLE IF NOT EXISTS run_courses (
                    run_id TEXT NOT NULL,
                    course_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error_message TEXT,
                    PRIMARY KEY (run_id, course_id)
                );
                CREATE TABLE IF NOT EXISTS vendor_locks (
                    vendor TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_runs_vendor_started
                    ON runs(vendor, started_at DESC);
                """
            )
            lock_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(vendor_locks)")
            }
            if "heartbeat_at" not in lock_columns:
                connection.execute("ALTER TABLE vendor_locks ADD COLUMN heartbeat_at TEXT")
            connection.execute(
                """
                UPDATE vendor_locks SET heartbeat_at = acquired_at
                WHERE heartbeat_at IS NULL
                """
            )

    async def is_ready(self) -> bool:
        try:
            return await asyncio.to_thread(self._is_ready)
        except sqlite3.Error:
            return False

    def _is_ready(self) -> bool:
        with self._connect() as connection:
            return bool(connection.execute("SELECT 1").fetchone()[0] == 1)

    async def acquire_lock(
        self, vendor: str, run_id: str, ttl_seconds: int = 3600
    ) -> None:
        await asyncio.to_thread(self._acquire_lock, vendor, run_id, ttl_seconds)

    def _acquire_lock(self, vendor: str, run_id: str, ttl_seconds: int) -> None:
        now = datetime.now(UTC)
        now_text = now.isoformat()
        stale_before = now - timedelta(seconds=ttl_seconds)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT run_id, COALESCE(heartbeat_at, acquired_at) AS last_seen_at
                FROM vendor_locks WHERE vendor = ?
                """,
                (vendor,),
            ).fetchone()
            if existing is not None:
                try:
                    last_seen_at = datetime.fromisoformat(str(existing["last_seen_at"]))
                    is_stale = last_seen_at < stale_before
                except (TypeError, ValueError):
                    is_stale = True
                if not is_stale:
                    raise JobAlreadyRunning(f"A {vendor} ingestion job is already running")
                stale_run_id = str(existing["run_id"])
                connection.execute(
                    """
                    UPDATE runs
                    SET status = ?, finished_at = ?, error_message = ?
                    WHERE run_id = ? AND status = ?
                    """,
                    (
                        RunStatus.FAILED.value,
                        now_text,
                        "Stale vendor lock was reclaimed",
                        stale_run_id,
                        RunStatus.RUNNING.value,
                    ),
                )
                connection.execute("DELETE FROM vendor_locks WHERE vendor = ?", (vendor,))
            connection.execute(
                """
                INSERT INTO vendor_locks(vendor, run_id, acquired_at, heartbeat_at)
                VALUES (?, ?, ?, ?)
                """,
                (vendor, run_id, now_text, now_text),
            )

    async def heartbeat_lock(self, vendor: str, run_id: str) -> None:
        await asyncio.to_thread(self._heartbeat_lock, vendor, run_id)

    def _heartbeat_lock(self, vendor: str, run_id: str) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE vendor_locks SET heartbeat_at = ?
                WHERE vendor = ? AND run_id = ?
                """,
                (_now(), vendor, run_id),
            )
            if cursor.rowcount != 1:
                raise JobLockLost(f"The {vendor} ingestion lock is no longer owned by {run_id}")

    async def release_lock(self, vendor: str, run_id: str) -> None:
        await asyncio.to_thread(self._release_lock, vendor, run_id)

    def _release_lock(self, vendor: str, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM vendor_locks WHERE vendor = ? AND run_id = ?", (vendor, run_id)
            )

    async def find_resumable_run(
        self, vendor: str, lock_ttl_seconds: int
    ) -> str | None:
        return await asyncio.to_thread(
            self._find_resumable_run, vendor, lock_ttl_seconds
        )

    def _find_resumable_run(self, vendor: str, lock_ttl_seconds: int) -> str | None:
        stale_before = (datetime.now(UTC) - timedelta(seconds=lock_ttl_seconds)).isoformat()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT runs.run_id
                FROM runs
                LEFT JOIN vendor_locks
                  ON vendor_locks.vendor = runs.vendor
                 AND vendor_locks.run_id = runs.run_id
                WHERE runs.vendor = ?
                  AND (
                    runs.status IN (?, ?)
                    OR (
                      runs.status = ?
                      AND (
                        vendor_locks.run_id IS NULL
                        OR COALESCE(vendor_locks.heartbeat_at, vendor_locks.acquired_at) < ?
                      )
                    )
                  )
                ORDER BY runs.started_at DESC
                LIMIT 1
                """,
                (
                    vendor,
                    RunStatus.FAILED.value,
                    RunStatus.PARTIAL_FAILURE.value,
                    RunStatus.RUNNING.value,
                    stale_before,
                ),
            ).fetchone()
            return str(row["run_id"]) if row else None

    async def purge_old_runs(self, vendor: str, retention_days: int) -> int:
        return await asyncio.to_thread(self._purge_old_runs, vendor, retention_days)

    def _purge_old_runs(self, vendor: str, retention_days: int) -> int:
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        terminal_statuses = (
            RunStatus.SUCCEEDED.value,
            RunStatus.PARTIAL_FAILURE.value,
            RunStatus.FAILED.value,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            resumable = connection.execute(
                """
                SELECT run_id FROM runs
                WHERE vendor = ? AND status IN (?, ?)
                ORDER BY started_at DESC LIMIT 1
                """,
                (
                    vendor,
                    RunStatus.FAILED.value,
                    RunStatus.PARTIAL_FAILURE.value,
                ),
            ).fetchone()
            resumable_run_id = str(resumable["run_id"]) if resumable else ""
            rows = connection.execute(
                """
                SELECT run_id FROM runs
                WHERE vendor = ?
                  AND status IN (?, ?, ?)
                  AND finished_at < ?
                  AND run_id != ?
                  AND NOT EXISTS (
                    SELECT 1 FROM vendor_locks
                    WHERE vendor_locks.vendor = runs.vendor
                      AND vendor_locks.run_id = runs.run_id
                  )
                """,
                (vendor, *terminal_statuses, cutoff, resumable_run_id),
            ).fetchall()
            run_ids = [(str(row["run_id"]),) for row in rows]
            if not run_ids:
                return 0
            connection.executemany("DELETE FROM checkpoints WHERE run_id = ?", run_ids)
            connection.executemany("DELETE FROM run_courses WHERE run_id = ?", run_ids)
            connection.executemany("DELETE FROM runs WHERE run_id = ?", run_ids)
            return len(run_ids)

    async def start_run(self, run_id: str, vendor: str) -> None:
        await asyncio.to_thread(self._start_run, run_id, vendor)

    def _start_run(self, run_id: str, vendor: str) -> None:
        now = _now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runs(run_id, vendor, status, started_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status = excluded.status,
                    finished_at = NULL,
                    error_message = NULL
                """,
                (run_id, vendor, RunStatus.RUNNING.value, now),
            )

    async def finish_run(
        self, run_id: str, status: RunStatus, error_message: str | None = None
    ) -> RunSummary:
        await asyncio.to_thread(self._finish_run, run_id, status, error_message)
        summary = await self.get_run(run_id)
        if summary is None:
            raise RuntimeError(f"Run {run_id} was not persisted")
        return summary

    def _finish_run(self, run_id: str, status: RunStatus, error_message: str | None) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = ?, finished_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                (status.value, _now(), error_message, run_id),
            )

    async def get_run(self, run_id: str) -> RunSummary | None:
        return await asyncio.to_thread(self._get_run, run_id)

    def _get_run(self, run_id: str) -> RunSummary | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return self._summary_from_row(connection, row) if row else None

    async def latest_run(self, vendor: str) -> RunSummary | None:
        return await asyncio.to_thread(self._latest_run, vendor)

    def _latest_run(self, vendor: str) -> RunSummary | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE vendor = ? ORDER BY started_at DESC LIMIT 1", (vendor,)
            ).fetchone()
            return self._summary_from_row(connection, row) if row else None

    @staticmethod
    def _summary_from_row(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> RunSummary:
        run_id = str(row["run_id"])
        checkpoint_totals = {
            item["data_domain"]: int(item["records_count"])
            for item in connection.execute(
                """
                SELECT data_domain, COALESCE(SUM(records_count), 0) AS records_count
                FROM checkpoints WHERE run_id = ? AND status = 'completed'
                GROUP BY data_domain
                """,
                (run_id,),
            )
        }
        course_totals = {
            item["status"]: int(item["count"])
            for item in connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM run_courses WHERE run_id = ? GROUP BY status
                """,
                (run_id,),
            )
        }
        return RunSummary(
            run_id=run_id,
            vendor=str(row["vendor"]),
            status=RunStatus(str(row["status"])),
            started_at=datetime.fromisoformat(str(row["started_at"])),
            finished_at=(
                datetime.fromisoformat(str(row["finished_at"])) if row["finished_at"] else None
            ),
            course_catalog_records=checkpoint_totals.get("course_catalog", 0),
            enrollment_records=checkpoint_totals.get("learning_history", 0),
            courses_succeeded=course_totals.get("completed", 0),
            courses_failed=course_totals.get("failed", 0),
            error_message=row["error_message"],
        )

    async def record_completed_page(
        self,
        run_id: str,
        data_domain: str,
        offset: int,
        records_count: int,
        course_id: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._record_completed_page,
            run_id,
            data_domain,
            offset,
            records_count,
            course_id or "",
        )

    def _record_completed_page(
        self,
        run_id: str,
        data_domain: str,
        offset: int,
        records_count: int,
        course_id: str,
    ) -> None:
        now = _now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO checkpoints(
                    run_id, vendor, data_domain, course_id, offset, status,
                    started_at, finished_at, records_count
                ) VALUES (?, 'levelup', ?, ?, ?, 'completed', ?, ?, ?)
                ON CONFLICT(run_id, data_domain, course_id, offset) DO UPDATE SET
                    status = 'completed',
                    finished_at = excluded.finished_at,
                    records_count = excluded.records_count,
                    error_message = NULL
                """,
                (run_id, data_domain, course_id, offset, now, now, records_count),
            )

    async def record_failed_page(
        self,
        run_id: str,
        data_domain: str,
        offset: int,
        error_message: str,
        course_id: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._record_failed_page,
            run_id,
            data_domain,
            offset,
            error_message,
            course_id or "",
        )

    def _record_failed_page(
        self,
        run_id: str,
        data_domain: str,
        offset: int,
        error_message: str,
        course_id: str,
    ) -> None:
        now = _now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO checkpoints(
                    run_id, vendor, data_domain, course_id, offset, status,
                    started_at, finished_at, records_count, error_message
                ) VALUES (?, 'levelup', ?, ?, ?, 'failed', ?, ?, 0, ?)
                ON CONFLICT(run_id, data_domain, course_id, offset) DO UPDATE SET
                    status = 'failed',
                    finished_at = excluded.finished_at,
                    records_count = 0,
                    error_message = excluded.error_message
                """,
                (run_id, data_domain, course_id, offset, now, now, error_message),
            )

    async def next_offset(
        self, run_id: str, data_domain: str, page_size: int, course_id: str | None = None
    ) -> int:
        return await asyncio.to_thread(
            self._next_offset, run_id, data_domain, page_size, course_id or ""
        )

    def _next_offset(
        self, run_id: str, data_domain: str, page_size: int, course_id: str
    ) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT MAX(offset) AS max_offset FROM checkpoints
                WHERE run_id = ? AND data_domain = ? AND course_id = ? AND status = 'completed'
                """,
                (run_id, data_domain, course_id),
            ).fetchone()
            return 0 if row["max_offset"] is None else int(row["max_offset"]) + page_size

    async def add_courses(self, run_id: str, course_ids: list[str]) -> None:
        if course_ids:
            await asyncio.to_thread(self._add_courses, run_id, course_ids)

    def _add_courses(self, run_id: str, course_ids: list[str]) -> None:
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO run_courses(run_id, course_id) VALUES (?, ?)",
                ((run_id, course_id) for course_id in course_ids),
            )

    async def courses_to_process(self, run_id: str) -> list[str]:
        return await asyncio.to_thread(self._courses_to_process, run_id)

    def _courses_to_process(self, run_id: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT course_id FROM run_courses
                WHERE run_id = ? AND status != 'completed'
                ORDER BY course_id
                """,
                (run_id,),
            ).fetchall()
            return [str(row["course_id"]) for row in rows]

    async def mark_course(
        self, run_id: str, course_id: str, status: str, error_message: str | None = None
    ) -> None:
        await asyncio.to_thread(self._mark_course, run_id, course_id, status, error_message)

    def _mark_course(
        self, run_id: str, course_id: str, status: str, error_message: str | None
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE run_courses SET status = ?, error_message = ?
                WHERE run_id = ? AND course_id = ?
                """,
                (status, error_message, run_id, course_id),
            )

    async def mark_catalog_completed(self, run_id: str) -> None:
        await asyncio.to_thread(self._mark_catalog_completed, run_id)

    def _mark_catalog_completed(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE runs SET catalog_completed = 1 WHERE run_id = ?", (run_id,))

    async def is_catalog_completed(self, run_id: str) -> bool:
        return await asyncio.to_thread(self._is_catalog_completed, run_id)

    def _is_catalog_completed(self, run_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT catalog_completed FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            return bool(row and row["catalog_completed"])


def _now() -> str:
    return datetime.now(UTC).isoformat()
