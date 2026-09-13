"""Import an audited, already-published vendor checkpoint without re-pulling."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

from azure.identity import AzureCliCredential
from azure.storage.blob import BlobClient

from app.core.config import Settings
from app.fabric_contract import DATASETS
from app.fabric_job import snapshot_database, target_id
from app.fabric_state import BlobState, pack_state
from app.models import RunStatus, RunSummary
from app.repositories import CheckpointStore


def _has_incremental_state(
    connection: sqlite3.Connection,
    vendor: str,
    domain: str,
) -> bool:
    if vendor.startswith("harvard_") and domain == "learning_history":
        query = "SELECT 1 FROM ingested_source_files WHERE vendor=? AND data_domain=?"
        row = connection.execute(query, (vendor, domain)).fetchone()
        return bool(row)
    scope = (
        " AND scope_key='full_sync'"
        if domain in {"assessment_history", "learning_history"}
        and vendor in {"skillup", "datacamp", "coursera", "linkedin"}
        else ""
    )
    query = "SELECT 1 FROM ingestion_watermarks WHERE vendor=? AND data_domain=?" + scope
    row = connection.execute(query, (vendor, domain)).fetchone()
    return bool(row)


def _validate_checkpoint_state(path: Path, vendor: str) -> None:
    with sqlite3.connect(path) as connection:
        if connection.execute(
            "SELECT 1 FROM vendor_locks WHERE vendor=?",
            (vendor,),
        ).fetchone():
            raise ValueError("Seed checkpoint still has an active vendor lock")
        for source_vendor, domain in DATASETS:
            if source_vendor != vendor or _has_incremental_state(connection, vendor, domain):
                continue
            if (vendor, domain) == ("coursera", "course_detail"):
                # Legacy Coursera checkpoints predate Course Detail ingestion.
                # Its missing watermark intentionally triggers the one-time backfill.
                continue
            raise ValueError(f"Seed missing incremental state: {vendor}/{domain}")
        if (
            vendor == "levelup"
            and not connection.execute(
                "SELECT 1 FROM vendor_entity_keys "
                "WHERE vendor='levelup' AND data_domain='course_catalog'"
            ).fetchone()
        ):
            raise ValueError("Seed missing LevelUP course inventory")


async def validate_seed(path: Path, vendor: str) -> RunSummary:
    store = CheckpointStore(path)
    await store.initialize()
    summary = await store.latest_run(vendor)
    if summary is None or summary.status != RunStatus.SUCCEEDED:
        raise ValueError("Seed requires a successful latest vendor run")
    _validate_checkpoint_state(path, vendor)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", required=True, choices=sorted({v for v, _ in DATASETS}))
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--confirm-already-published", action="store_true", required=True)
    args = parser.parse_args()
    settings = Settings()
    settings.validate_fabric_runtime()
    if not args.checkpoint.is_file():
        parser.error("Checkpoint file does not exist")
    with TemporaryDirectory(prefix="fabric-seed-") as temp:
        directory = Path(temp)
        snapshot_database(args.checkpoint, directory / "checkpoint.db")
        summary = asyncio.run(validate_seed(directory / "checkpoint.db", args.vendor))
        descriptor: dict[str, object] = {
            "version": 1,
            "phase": "committed",
            "generation": 0,
            "target": target_id(settings, args.vendor),
            "tables": {},
            "summary": summary.model_dump(mode="json"),
        }
        (directory / "state.json").write_text(json.dumps(descriptor))
        archive = directory / "seed.zip"
        pack_state(directory, archive, pending=False)
        with (
            AzureCliCredential() as credential,
            BlobClient(
                settings.fabric_state_account_url,
                settings.fabric_state_container,
                f"{target_id(settings, args.vendor)}/state.zip",
                credential=credential,
            ) as blob,
            BlobState(blob) as state,
        ):
            if state.download(directory / "existing.zip"):
                raise RuntimeError("Durable checkpoint already exists; seed cannot overwrite it")
            state.upload(archive)
        print(f"Seeded durable checkpoint for {args.vendor}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Seed failed: {type(exc).__name__}; no secret values logged") from None
