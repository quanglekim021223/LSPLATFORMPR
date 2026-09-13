"""Scheduled vendor ingestion: prepare locally, publish Delta, commit checkpoint."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient

from app.core.config import Settings
from app.fabric_contract import DATASETS
from app.fabric_state import BlobState, pack_state, unpack_state
from app.fabric_tables import (
    append_table,
    build_batch,
    reconcile_table,
    table_state,
)
from app.models import RunStatus, RunSummary
from app.repositories import CheckpointStore, LocalBronzeWriter

Runner = Callable[..., Awaitable[object]]
logger = logging.getLogger(__name__)
STATE_FILE = "state.json"


def target_id(settings: Settings, vendor: str) -> str:
    return (
        f"{settings.fabric_workspace_id}/{settings.fabric_lakehouse_id}/"
        f"{settings.fabric_schema}/{vendor}"
    )


def snapshot_database(source: Path, destination: Path) -> None:
    # SQLite backup includes WAL contents; copying checkpoint.db alone does not.
    with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst)


async def prepare(
    settings: Settings,
    vendor: str,
    runner: Runner,
    directory: Path,
    generation: int,
    delta_tables: dict[str, dict[str, int]],
) -> dict[str, Any]:
    working = directory / "working.db"
    committed = directory / "checkpoint.db"
    if committed.exists():
        snapshot_database(committed, working)
    config = settings.model_copy(
        update={
            "checkpoint_db_path": working,
            "bronze_local_path": directory / "raw",
            "bronze_storage_type": "local",
            "scheduler_enabled": False,
            "history_periodic_resync_enabled": False,
            "skillup_page_size": min(settings.skillup_page_size, 50),
        }
    )
    result = await runner(
        config,
        checkpoint_store=CheckpointStore(working),
        bronze_writer=LocalBronzeWriter(directory / "raw"),
    )
    if not isinstance(result, RunSummary) or result.status != RunStatus.SUCCEEDED:
        raise RuntimeError(f"Vendor ingestion did not complete: {vendor}")
    counts = await asyncio.to_thread(
        build_batch,
        directory / "raw",
        directory / "tables",
        vendor,
        result.run_id,
    )
    for (source_vendor, domain), (_, mappings) in DATASETS.items():
        if source_vendor == vendor:
            actual = sum(counts.get(table, 0) for table, _ in mappings)
            if actual != result.records_by_domain.get(domain, 0):
                raise ValueError(f"Prepared count differs from ingestion checkpoint: {domain}")
    # The durable state is still the old checkpoint until the complete batch is prepared.
    await asyncio.to_thread(snapshot_database, working, committed)
    return {
        "version": 2,
        "phase": "pending",
        "target": target_id(settings, vendor),
        "generation": generation,
        "delta_tables": delta_tables,
        "tables": counts,
        "summary": result.model_dump(mode="json"),
    }


def execute(
    settings: Settings,
    vendor: str,
    runner: Runner,
    directory: Path,
    state: Any,
    publish: Callable[[Path, dict[str, Any]], None],
) -> RunSummary:
    archive = directory / "state.zip"
    found = state.download(archive)
    if found:
        unpack_state(archive, directory)
        descriptor = json.loads((directory / STATE_FILE).read_text())
        if (
            descriptor.get("version") not in {1, 2}
            or descriptor.get("target") != target_id(settings, vendor)
            or descriptor.get("phase") not in {"committed", "pending"}
        ):
            raise ValueError("Checkpoint version or Fabric target mismatch")
    else:
        if not settings.fabric_allow_initial_pull:
            raise RuntimeError(f"Missing durable checkpoint for {vendor}; seed before scheduling")
        descriptor = {"phase": "committed", "generation": 0}
    if descriptor["phase"] == "committed":
        descriptor = asyncio.run(
            prepare(
                settings,
                vendor,
                runner,
                directory,
                int(descriptor["generation"]) + 1,
                descriptor.get("delta_tables", {}),
            )
        )
        (directory / STATE_FILE).write_text(json.dumps(descriptor))
        pack_state(directory, archive, pending=True)
        state.upload(archive)
    # Pending state survives process loss, publish failure, or an ambiguous response.
    state.check()
    publish(directory, descriptor)
    state.check()
    descriptor["phase"] = "committed"
    (directory / STATE_FILE).write_text(json.dumps(descriptor))
    pack_state(directory, archive, pending=False)
    state.upload(archive)
    return RunSummary.model_validate(descriptor["summary"])


def publish_tables(
    directory: Path,
    descriptor: dict[str, Any],
    *,
    settings: Settings,
    vendor: str,
    state: BlobState,
    credential: ManagedIdentityCredential,
) -> None:
    tables = sorted(
        {
            table
            for (source_vendor, _), (_, mappings) in DATASETS.items()
            if source_vendor == vendor
            for table, _ in mappings
        }
    )
    delta_tables = descriptor.setdefault("delta_tables", {})
    restored_records = 0
    for table in tables:
        state.check()
        # Renew credentials between table commits, not once per long-running pull.
        token = credential.get_token("https://storage.azure.com/.default")
        bronze_uri = (
            f"abfss://{settings.fabric_workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{settings.fabric_lakehouse_id}/Tables/{settings.fabric_schema}/{table}"
        )
        storage_options = {"bearer_token": token.token, "use_fabric_endpoint": "true"}
        bronze_state_key = f"bronze/{table}"
        bronze_app_id = f"lsplatform:{target_id(settings, vendor)}"
        restored = reconcile_table(
            bronze_uri,
            delta_tables.get(bronze_state_key),
            app_id=bronze_app_id,
            generation=descriptor["generation"],
            storage_options=storage_options,
        )
        restored_records += restored
        if restored:
            logger.warning(
                "Fabric Bronze DELETE restored vendor=%s table=%s rows=%d",
                vendor,
                table,
                restored,
            )
        count = descriptor["tables"].get(table)
        if count is not None:
            append_table(
                bronze_uri,
                directory / "tables",
                table,
                count,
                bronze_app_id,
                descriptor["generation"],
                storage_options,
            )
            logger.info(
                "Fabric Bronze committed vendor=%s table=%s rows=%d", vendor, table, count
            )
        bronze_state = table_state(bronze_uri, storage_options)
        if bronze_state is not None:
            delta_tables[bronze_state_key] = bronze_state
    descriptor["summary"]["restored_records"] = restored_records


def _run(settings: Settings, vendor: str, runner: Runner) -> RunSummary:
    settings.validate_fabric_runtime()
    identity = settings.fabric_managed_identity_client_id
    with ManagedIdentityCredential(client_id=identity or None) as credential:
        blob_name = f"{target_id(settings, vendor)}/state.zip"
        with (
            BlobClient(
                settings.fabric_state_account_url,
                settings.fabric_state_container,
                blob_name,
                credential=credential,
                connection_timeout=15,
                read_timeout=60,
            ) as blob,
            TemporaryDirectory(prefix="fabric-ingest-") as temp,
            BlobState(blob) as state,
        ):
            publish = partial(
                publish_tables,
                settings=settings,
                vendor=vendor,
                state=state,
                credential=credential,
            )
            return execute(settings, vendor, runner, Path(temp), state, publish)


async def run_fabric_ingestion(settings: Settings, vendor: str, runner: Runner) -> RunSummary:
    task = asyncio.create_task(asyncio.to_thread(_run, settings, vendor, runner))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A synchronous Delta write cannot be cancelled. Retain its lease until it settles.
        try:
            await task
        except Exception:
            logger.error("Fabric worker failed during cancellation vendor=%s", vendor)
        raise
    except Exception as exc:
        # SDK exception text can contain request credentials or source values.
        logger.exception(
            "Fabric ingestion failed vendor=%s error_type=%s", vendor, type(exc).__name__
        )
        raise RuntimeError(f"Fabric ingestion failed for {vendor}: {type(exc).__name__}") from None
