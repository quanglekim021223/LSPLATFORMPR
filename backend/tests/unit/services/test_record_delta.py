from __future__ import annotations

from pathlib import Path

import pytest

from app.repositories import CheckpointStore
from app.services.record_delta import RecordDelta


@pytest.mark.asyncio
async def test_record_delta_selects_only_new_and_updated_records(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state.db")
    await store.initialize()
    initial = [{"id": f"course-{index}", "title": f"Course {index}"} for index in range(100)]

    delta = await RecordDelta.load(store, "datacamp", "datacamp_course_catalog_live")
    first = delta.select(initial)
    assert len(first.indexes) == 100
    await delta.commit(first, "run-1")

    with_new = [*initial, {"id": "course-new", "title": "New course"}]
    delta = await RecordDelta.load(store, "datacamp", "datacamp_course_catalog_live")
    added = delta.select(with_new)
    assert added.indexes == (100,)
    await delta.commit(added, "run-2")

    with_update = [dict(record) for record in with_new]
    with_update[25]["title"] = "Updated course"
    delta = await RecordDelta.load(store, "datacamp", "datacamp_course_catalog_live")
    updated = delta.select(with_update)
    assert updated.indexes == (25,)
