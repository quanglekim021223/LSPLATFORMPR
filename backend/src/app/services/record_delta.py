from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from app.fabric_contract import canonical
from app.repositories import CheckpointStore

_SOURCE_KEYS: dict[str, tuple[str, ...]] = {
    "skillup_learning_resources": ("learningMaterialId",),
    "skillup_certificates": ("certificateId",),
    "datacamp_course_catalog_live": ("id",),
    "datacamp_course_catalog_archived": ("id",),
    "harvard_hmm_learning_history": ("EventDate", "Username", "EventName", "Product"),
    "harvard_spark_learning_history": (
        "Event Date",
        "Username",
        "Event Name",
        "Product ID",
    ),
    "fams_training_classes": ("id",),
    "fams_training_students": ("courseCode", "account"),
}


@dataclass(frozen=True, slots=True)
class RecordSelection:
    indexes: tuple[int, ...]
    fingerprints: dict[str, str]


class RecordDelta:
    def __init__(
        self,
        checkpoints: CheckpointStore,
        vendor: str,
        table: str,
        known: dict[str, str],
    ) -> None:
        self._checkpoints = checkpoints
        self._vendor = vendor
        self._table = table
        self._known = known

    @property
    def has_state(self) -> bool:
        return bool(self._known)

    @classmethod
    async def load(
        cls,
        checkpoints: CheckpointStore,
        vendor: str,
        table: str,
    ) -> RecordDelta:
        return cls(
            checkpoints,
            vendor,
            table,
            await checkpoints.entity_fingerprints(vendor, table),
        )

    def select(self, records: list[dict[str, Any]]) -> RecordSelection:
        indexes: list[int] = []
        changed: dict[str, str] = {}
        seen: set[str] = set()
        for index, record in enumerate(records):
            key = _record_key(self._table, record)
            if key in seen:
                raise ValueError(f"Duplicate business key in {self._table}")
            seen.add(key)
            fingerprint = hashlib.sha256(canonical(record).encode("utf-8")).hexdigest()
            if self._known.get(key) != fingerprint:
                indexes.append(index)
                changed[key] = fingerprint
        return RecordSelection(tuple(indexes), changed)

    async def commit(self, selection: RecordSelection, run_id: str) -> None:
        await self._checkpoints.remember_entity_fingerprints(
            self._vendor,
            self._table,
            selection.fingerprints,
            run_id,
        )
        self._known.update(selection.fingerprints)


def _record_key(table: str, record: dict[str, Any]) -> str:
    fields = _SOURCE_KEYS[table]
    values: list[Any] = []
    for field in fields:
        if field not in record or record[field] is None or record[field] == "":
            raise ValueError(f"Missing business key field {field} in {table}")
        values.append(record[field])
    return canonical(values)
