from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4
from zipfile import ZipFile

import pytest
from deltalake import DeltaTable

from app.core.config import Settings
from app.fabric_job import execute, publish_tables
from app.fabric_state import unpack_state
from app.fabric_tables import (
    append_table,
    reconcile_table,
    table_state,
)
from app.models import PageWrite, RunStatus

TABLE = "levelup_course_catalog"


class State:
    def __init__(self):
        self.payload = None
        self.fail_commit = False

    def check(self):
        pass

    def download(self, path):
        if self.payload is None:
            return False
        path.write_bytes(self.payload)
        return True

    def upload(self, path):
        with ZipFile(path) as archive:
            phase = json.loads(archive.read("state.json"))["phase"]
        if self.fail_commit and phase == "committed":
            raise RuntimeError("checkpoint unavailable")
        self.payload = path.read_bytes()


def runner_for(records, calls, *, status=RunStatus.SUCCEEDED, write=True):
    async def runner(config, checkpoint_store, bronze_writer):
        calls.append(await read_watermark(checkpoint_store))
        assert config.history_periodic_resync_enabled is False
        run_id = str(uuid4())
        await checkpoint_store.start_run(run_id, "levelup")
        if write:
            await bronze_writer.write_page(
                PageWrite(
                    vendor="levelup",
                    data_domain="course_catalog",
                    ingestion_date="2026-09-11",
                    run_id=run_id,
                    offset=0,
                    raw_payload=json.dumps({"courses": records}).encode(),
                    records_count=len(records),
                    request_parameters={},
                    fetched_at=datetime.now(UTC),
                )
            )
        await checkpoint_store.record_completed_page(run_id, "course_catalog", 0, len(records))
        await checkpoint_store.set_watermark("levelup", "course_catalog", str(len(calls)), run_id)
        return await checkpoint_store.finish_run(run_id, status)

    return runner


async def read_watermark(store):
    await store.initialize()
    return await store.get_watermark("levelup", "course_catalog")


def execute_at(tmp_path, state, runner, publisher, allow=True):
    tmp_path.mkdir()
    config = Settings(_env_file=None, fabric_allow_initial_pull=allow)
    return execute(config, "levelup", runner, tmp_path, state, publisher)


def publisher_at(root, *, fail_after_write=False):
    def publish(directory, descriptor):
        states = descriptor.setdefault("delta_tables", {})
        restored = reconcile_table(
            str(root / TABLE),
            states.get(f"bronze/{TABLE}"),
            app_id="test-levelup",
            generation=descriptor["generation"],
        )
        for table, count in descriptor["tables"].items():
            append_table(
                str(root / table),
                directory / "tables",
                table,
                count,
                "test-levelup",
                descriptor["generation"],
            )
        states[f"bronze/{TABLE}"] = table_state(str(root / TABLE))
        descriptor["summary"]["restored_records"] = restored
        if fail_after_write:
            raise RuntimeError("lost response after Delta commit")

    return publish


def no_op_publisher(*_args):
    pass


def test_incremental_run_restores_deleted_bronze_record_without_full_pull(tmp_path):
    state, calls = State(), []
    records = [{"id": str(index), "name": "original"} for index in range(50_000)]
    publish = publisher_at(tmp_path / "fabric")

    execute_at(tmp_path / "first", state, runner_for(records, calls), publish)
    delta = DeltaTable(tmp_path / "fabric" / TABLE)
    delta.delete("id = '12345'")
    delta.update(updates={"name": "'tampered'"}, predicate="id = '23456'")
    assert delta.count() == 49_999

    result = execute_at(tmp_path / "second", state, runner_for([], calls), publish)
    repaired = DeltaTable(tmp_path / "fabric" / TABLE).to_pyarrow_table()

    assert calls == [None, "1"]
    assert result.records_by_domain == {"course_catalog": 0}
    assert result.restored_records == 1
    assert repaired.num_rows == 50_000
    assert repaired.column("id").to_pylist().count("12345") == 1


def test_empty_incremental_batch_still_checks_all_vendor_tables_for_deletes(
    tmp_path, monkeypatch
):
    checked = []

    def reconcile(uri, _expected, **_kwargs):
        checked.append(uri)
        return int(uri.endswith(TABLE))

    monkeypatch.setattr("app.fabric_job.reconcile_table", reconcile)
    monkeypatch.setattr("app.fabric_job.table_state", lambda *_args: None)
    descriptor = {"tables": {}, "summary": {}, "generation": 2}
    settings = Settings(
        _env_file=None,
        fabric_workspace_id="workspace",
        fabric_lakehouse_id="lakehouse",
    )
    credential = SimpleNamespace(
        get_token=lambda _scope: SimpleNamespace(token="test-token")
    )

    publish_tables(
        tmp_path,
        descriptor,
        settings=settings,
        vendor="levelup",
        state=State(),
        credential=credential,
    )

    assert {uri.rsplit("/", 1)[-1] for uri in checked} == {
        "levelup_course_catalog",
        "levelup_learning_history",
    }
    assert descriptor["summary"]["restored_records"] == 1


def test_checkpoint_and_delta_retry_after_ambiguous_publish(tmp_path):
    state, calls = State(), []
    records = [{"id": str(i), "name": "original", "skills": [1, 2]} for i in range(50)]
    first = runner_for(records, calls)
    publish = publisher_at(tmp_path / "fabric")
    failing_publish = publisher_at(tmp_path / "fabric", fail_after_write=True)
    with pytest.raises(RuntimeError, match="lost response"):
        execute_at(tmp_path / "first", state, first, failing_publish)
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 50
    # A new process/temp directory replays the pending batch without re-pulling.
    execute_at(tmp_path / "retry", state, first, publish)
    assert calls == [None]
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 50
    edits = [{"id": str(i), "name": "edited", "new_field": None} for i in range(5)]
    execute_at(tmp_path / "second", state, runner_for(edits, calls), publish)
    assert calls == [None, "1"]
    delta = DeltaTable(tmp_path / "fabric" / TABLE)
    assert delta.count() == 55  # Bronze preserves original records and their new versions.
    assert delta.transaction_version("test-levelup") == 2
    table = delta.to_pyarrow_table()
    assert "new_field" in table.column_names
    assert {
        "_ingested_at",
        "_run_id",
        "_source_vendor",
        "_source_domain",
    }.issubset(table.column_names)
    assert table.column("_source_vendor").to_pylist() == ["levelup"] * 55
    assert table.column("_source_domain").to_pylist() == ["course_catalog"] * 55
    assert len(set(table.column("_run_id").to_pylist())) == 2
    execute_at(tmp_path / "empty", state, runner_for([], calls), publish)
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 55


def test_checkpoint_failure_retries_without_appending_twice(tmp_path):
    state, calls = State(), []
    state.fail_commit = True
    runner = runner_for([{"id": "1"}], calls)
    publish = publisher_at(tmp_path / "fabric")
    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        execute_at(tmp_path / "first", state, runner, publish)
    state.fail_commit = False
    execute_at(tmp_path / "retry", state, runner, publish)
    assert calls == [None]
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 1


@pytest.mark.parametrize("status", [RunStatus.FAILED, RunStatus.PARTIAL_FAILURE])
def test_partial_ingestion_never_publishes_or_advances_checkpoint(tmp_path, status):
    state, calls = State(), []
    publish = publisher_at(tmp_path / "fabric")
    execute_at(tmp_path / "first", state, runner_for([{"id": "1"}], calls), publish)
    committed = state.payload
    failed_runner = runner_for([{"id": "2"}], calls, status=status)
    with pytest.raises(RuntimeError, match="did not complete"):
        execute_at(tmp_path / "failed", state, failed_runner, publish)
    assert state.payload == committed
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 1


def test_missing_checkpoint_does_not_accidentally_full_pull(tmp_path):
    state, calls = State(), []
    runner = runner_for([], calls)
    with pytest.raises(RuntimeError, match="Missing durable checkpoint"):
        execute_at(tmp_path / "first", state, runner, no_op_publisher, allow=False)
    assert calls == []


def test_missing_raw_fails_before_checkpoint_commit(tmp_path):
    state, calls = State(), []
    runner = runner_for([{"id": "1"}], calls, write=False)
    with pytest.raises(ValueError, match="Prepared count differs"):
        execute_at(tmp_path / "missing", state, runner, no_op_publisher)
    assert state.payload is None


def test_checkpoint_archive_rejects_traversal(tmp_path):
    archive = tmp_path / "bad.zip"
    with ZipFile(archive, "w") as output:
        output.writestr("../checkpoint.db", b"bad")
    with pytest.raises(ValueError, match="Invalid checkpoint archive"):
        unpack_state(archive, tmp_path / "out")


def test_duplicate_source_records_are_preserved(tmp_path):
    state, calls = State(), []
    execute_at(
        tmp_path / "first",
        state,
        runner_for([{"id": "1"}] * 2, calls),
        publisher_at(tmp_path / "fabric"),
    )
    assert DeltaTable(tmp_path / "fabric" / TABLE).count() == 2
