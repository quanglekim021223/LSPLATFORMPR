"""Import an audited, already-published vendor checkpoint without re-pulling."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from azure.identity import AzureCliCredential
from azure.storage.blob import BlobClient

from app.core.config import Settings
from app.fabric_contract import DATASETS
from app.fabric_job import snapshot_database, target_id
from app.fabric_seed import validate_seed
from app.fabric_state import BlobState, pack_state


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
        print(
            f"Seeded durable Fabric checkpoint for {args.vendor} "
            f"from run {summary.run_id}"
        )


def cli() -> None:
    try:
        main()
    except Exception as exc:
        raise SystemExit(
            f"Seed failed: {type(exc).__name__}; no secret values logged"
        ) from None


if __name__ == "__main__":
    cli()
