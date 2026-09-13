from __future__ import annotations

import pytest

from app.fabric_seed import validate_seed
from app.models import RunStatus
from app.repositories import CheckpointStore


async def datacamp_checkpoint(path, *, full_sync=True, status=RunStatus.SUCCEEDED):
    store = CheckpointStore(path)
    await store.initialize()
    await store.start_run("seed-test", "datacamp")
    for domain in ("course_catalog_live", "course_catalog_archived", "learning_history"):
        await store.set_watermark("datacamp", domain, "2026-09-11T00:00:00Z", "seed-test")
    if full_sync:
        await store.set_watermark(
            "datacamp", "learning_history", "2026-09-11T00:00:00Z", "seed-test", "full_sync"
        )
    await store.finish_run("seed-test", status)
    return store


async def test_seed_accepts_complete_successful_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.db"
    await datacamp_checkpoint(path)
    summary = await validate_seed(path, "datacamp")
    assert summary.run_id == "seed-test"


async def test_seed_rejects_history_without_full_sync_scope(tmp_path):
    path = tmp_path / "checkpoint.db"
    await datacamp_checkpoint(path, full_sync=False)
    with pytest.raises(ValueError, match="Missing|missing incremental state"):
        await validate_seed(path, "datacamp")


async def test_seed_rejects_partial_vendor_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.db"
    await datacamp_checkpoint(path, status=RunStatus.PARTIAL_FAILURE)
    with pytest.raises(ValueError, match="successful latest vendor run"):
        await validate_seed(path, "datacamp")


async def test_seed_accepts_legacy_coursera_checkpoint_without_detail_watermark(tmp_path):
    path = tmp_path / "checkpoint.db"
    store = CheckpointStore(path)
    await store.initialize()
    await store.start_run("seed-test", "coursera")
    await store.set_watermark("coursera", "course_catalog", "1700000000", "seed-test")
    await store.set_watermark(
        "coursera",
        "learning_history",
        "1700000000000",
        "seed-test",
        "full_sync",
    )
    await store.finish_run("seed-test", RunStatus.SUCCEEDED)

    summary = await validate_seed(path, "coursera")

    assert summary.run_id == "seed-test"
    assert await store.get_watermark("coursera", "course_detail") is None
