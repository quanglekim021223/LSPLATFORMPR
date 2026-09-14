"""Audit and validate already-published vendor checkpoints."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from app.fabric_contract import DATASETS
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
    """Preserve the original module entrypoint after moving the CLI to scripts."""
    from scripts.fabric_seed import cli

    cli()


if __name__ == "__main__":
    main()
