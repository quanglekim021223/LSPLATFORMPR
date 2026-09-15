"""Source-shaped Bronze batches and retry-safe Delta append commits."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake.exceptions import TableNotFoundError

from app.fabric_contract import DATASETS, TABLES, canonical, records_from_bytes


def column_name(value: object) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")
    if not name:
        raise ValueError("Source column has no SQL-safe characters")
    return name


def _table_value(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return canonical(value)


def table_row(
    record: dict[str, Any],
    *,
    ingested_at: str,
    run_id: str,
    source_vendor: str,
    source_domain: str,
    source_file: str | None = None,
) -> dict[str, str | None]:
    row: dict[str, str | None] = {}
    for key, value in record.items():
        name = column_name(key)
        if name in row:
            raise ValueError("Source columns collide after SQL name conversion")
        row[name] = _table_value(value)
    row.update(
        {
            "_ingested_at": ingested_at,
            "_run_id": run_id,
            "_source_vendor": source_vendor,
            "_source_domain": source_domain,
        }
    )
    if source_file is not None:
        row["_source_file"] = source_file
    return row


def entry_ingested_at(entry: dict[str, Any]) -> str:
    value = entry.get("fetched_at") or entry.get("downloaded_at")
    if not isinstance(value, str) or not value:
        raise ValueError("Raw manifest entry is missing its ingestion timestamp")
    return value


def source_file_type(mappings: tuple[tuple[str, str], ...]) -> str:
    formats = {"csv" if record_path == "csv" else "json" for _, record_path in mappings}
    if len(formats) != 1:
        raise ValueError("Bronze domain mixes source file types")
    return formats.pop()


def _skillup_skills(record: dict[str, Any]) -> list[dict[str, Any]]:
    skills = record.get("skills")
    if not isinstance(skills, list) or any(not isinstance(skill, dict) for skill in skills):
        raise ValueError("SkillUp snapshot record has invalid skills")
    return skills


def _skill_values(skills: list[dict[str, Any]], field: str) -> list[Any]:
    return [skill.get(field) for skill in skills]


def _skillup_shared(skills: list[dict[str, Any]], fetched_at: str) -> dict[str, Any]:
    return {
        "fetched_at": fetched_at,
        "skill_ids": _skill_values(skills, "taxonomySkillId"),
        "skill_names": _skill_values(skills, "skillName"),
        "skill_descriptions": _skill_values(skills, "description"),
        "skill_explanations": _skill_values(skills, "explanation"),
        "skill_proficiencies": _skill_values(skills, "proficiency"),
    }


def _skillup_certificate(record: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    return {
        "certificate_id": record.get("certificateId"),
        "title": record.get("title"),
        "issuer": record.get("issuer"),
        "certificate_status": record.get("certificateStatus"),
        **_skillup_shared(_skillup_skills(record), fetched_at),
    }


def _skillup_learning_resource(record: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    issuer = record.get("issuer") or {}
    material_type = record.get("learningMaterialType") or {}
    if not isinstance(issuer, dict) or not isinstance(material_type, dict):
        raise ValueError("SkillUp learning resource has invalid nested objects")
    skills = _skillup_skills(record)
    return {
        "learning_material_id": record.get("learningMaterialId"),
        "external_material_id": record.get("externalMaterialId"),
        "title": record.get("title"),
        "url": record.get("url"),
        "recommendation_type": record.get("recommendationType"),
        "issuer_id": issuer.get("id"),
        "material_type_id": material_type.get("id"),
        "issuer_name": issuer.get("name"),
        "material_type_name": material_type.get("name"),
        "skill_sources": _skill_values(skills, "source"),
        **_skillup_shared(skills, fetched_at),
    }


def bronze_record(table: str, record: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    """Keep the two pre-existing SkillUp table contracts stable."""
    if table == "skillup_certificates":
        return _skillup_certificate(record, fetched_at)
    if table == "skillup_learning_resources":
        return _skillup_learning_resource(record, fetched_at)
    return record


def arrow_table(rows: list[dict[str, str | None]]) -> pa.Table:
    names = sorted({name for row in rows for name in row})
    return pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in names]))


def _dataset_mappings(vendor: str, domain: str) -> tuple[tuple[str, str], ...] | None:
    key = (vendor, domain)
    if key in DATASETS:
        return DATASETS[key][1]
    # Catalog detail requests are not separate Bronze endpoint contracts.
    if domain == "learning_asset_detail":
        return None
    raise ValueError("Unmapped Bronze domain")


def _verified_raw(manifest_path: Path, entry: dict[str, Any]) -> bytes:
    name = entry["file"]
    if not isinstance(name, str) or Path(name).name != name:
        raise ValueError("Invalid raw page path")
    raw = (manifest_path.parent / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
        raise ValueError("Raw page checksum mismatch")
    return raw


def _write_records(
    records: list[dict[str, Any]],
    output: Path,
    table: str,
    counts: dict[str, int],
    *,
    ingested_at: str,
    run_id: str,
    vendor: str,
    domain: str,
    source_file: str | None,
) -> None:
    destination = output / table
    destination.mkdir(parents=True, exist_ok=True)
    # Bound Arrow allocation even for a large SFTP file.
    for offset in range(0, len(records), 25_000):
        chunk = [
            table_row(
                bronze_record(table, row, ingested_at),
                ingested_at=ingested_at,
                run_id=run_id,
                source_vendor=vendor,
                source_domain=domain,
                source_file=source_file,
            )
            for row in records[offset : offset + 25_000]
        ]
        part = destination / f"part-{counts.get(table, 0):012d}.parquet"
        pq.write_table(arrow_table(chunk), part)
        counts[table] = counts.get(table, 0) + len(chunk)


def _selected_records(
    records: list[dict[str, Any]],
    indexes: object,
) -> list[dict[str, Any]]:
    if indexes is None:
        return records
    if (
        not isinstance(indexes, list)
        or any(not isinstance(index, int) for index in indexes)
        or any(index < 0 or index >= len(records) for index in indexes)
        or len(indexes) != len(set(indexes))
    ):
        raise ValueError("Invalid selected record index")
    return [records[index] for index in indexes]


def _build_entry(
    manifest_path: Path,
    entry: dict[str, Any],
    mappings: tuple[tuple[str, str], ...],
    output: Path,
    counts: dict[str, int],
    *,
    run_id: str,
    vendor: str,
    domain: str,
) -> None:
    raw = _verified_raw(manifest_path, entry)
    ingested_at = entry_ingested_at(entry)
    source_file = source_file_type(mappings)
    total = 0
    source_total = 0
    selected = entry.get("selected_record_indexes")
    if selected is not None and not isinstance(selected, dict):
        raise ValueError("Invalid raw record selection")
    for table, record_path in mappings:
        if not raw:
            continue
        records = records_from_bytes(raw, record_path)
        source_total += len(records)
        indexes = selected.get(record_path) if selected is not None else None
        records = _selected_records(records, indexes)
        total += len(records)
        if records:
            _write_records(
                records,
                output,
                table,
                counts,
                ingested_at=ingested_at,
                run_id=run_id,
                vendor=vendor,
                domain=domain,
                source_file=source_file,
            )
    expected_source_total = entry.get("source_records_count", source_total)
    if source_total != expected_source_total:
        raise ValueError("Raw source record count mismatch")
    if total != entry["records_count"]:
        raise ValueError("Raw record count mismatch")


def build_batch(raw_root: Path, output: Path, vendor: str, run_id: str) -> dict[str, int]:
    """Verify local pages, keeping every record and preserving nested payloads."""
    counts: dict[str, int] = {}
    for manifest_path in sorted(raw_root.glob(f"{vendor}/**/manifest.json")):
        manifest = json.loads(manifest_path.read_text())
        if manifest["run_id"] != run_id:
            continue
        domain = manifest["data_domain"]
        mappings = _dataset_mappings(vendor, domain)
        if mappings is None:
            continue
        entries = manifest.get("pages", []) + manifest.get("files", [])
        for entry in entries:
            _build_entry(
                manifest_path,
                entry,
                mappings,
                output,
                counts,
                run_id=run_id,
                vendor=vendor,
                domain=domain,
            )
    return counts


def append_table(
    uri: str,
    batch: Path,
    table: str,
    expected_rows: int,
    app_id: str,
    generation: int,
    storage_options: dict[str, str] | None = None,
) -> None:
    """One Delta transaction per table; retry uses the same durable generation."""
    if table not in TABLES:
        raise ValueError("Unapproved Bronze table")
    paths = sorted(str(path) for path in (batch / table).glob("*.parquet"))
    if not paths:
        raise ValueError("Missing prepared Parquet batch")
    schema = pa.unify_schemas([pq.read_schema(path) for path in paths])
    dataset = ds.dataset(paths, format="parquet", schema=schema)
    if dataset.count_rows() != expected_rows:
        raise ValueError("Prepared table row count mismatch")
    try:
        delta = DeltaTable(uri, storage_options=storage_options)
    except TableNotFoundError:
        delta = None
    if delta is not None:
        previous = delta.transaction_version(app_id)
        if previous == generation:
            return
        if previous is not None and previous > generation:
            raise ValueError("Checkpoint is older than the published Delta transaction")
    write_deltalake(
        delta if delta is not None else uri,
        dataset.scanner(batch_size=25_000).to_reader(),
        mode="append",
        schema_mode="merge",
        name=table,
        storage_options=storage_options,
        commit_properties=CommitProperties(
            app_transactions=[Transaction(app_id, generation)],
            max_commit_retries=0,
        ),
    )
    if DeltaTable(uri, storage_options=storage_options).transaction_version(app_id) != generation:
        raise RuntimeError("Delta transaction verification failed")


def table_state(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> dict[str, int] | None:
    try:
        delta = DeltaTable(uri, storage_options=storage_options)
    except TableNotFoundError:
        return None
    return {"version": delta.version(), "rows": delta.count()}


def reconcile_table(
    uri: str,
    expected: dict[str, int] | None,
    *,
    app_id: str,
    generation: int,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Restore a committed table version after an out-of-band mutation."""
    try:
        delta = DeltaTable(uri, storage_options=storage_options)
    except TableNotFoundError:
        if expected is not None:
            raise RuntimeError("Committed Delta table is missing") from None
        return 0
    current = delta.version()
    if expected is None:
        return restore_external_delete(uri, storage_options)
    committed = expected["version"]
    if current < committed:
        raise RuntimeError("Delta table is older than its committed checkpoint")
    if current == committed or delta.transaction_version(app_id) == generation:
        return 0
    before = delta.count()
    delta.restore(committed)
    after = delta.count()
    return max(after - before, 0)


def restore_external_delete(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Undo the latest DELETE because Bronze tables are append-only."""
    try:
        delta = DeltaTable(uri, storage_options=storage_options)
    except TableNotFoundError:
        return 0
    if delta.version() == 0:
        return 0
    latest = delta.history(1)[0]
    if latest.get("operation") != "DELETE":
        return 0
    metrics = latest.get("operationMetrics")
    deleted = metrics.get("num_deleted_rows", 0) if isinstance(metrics, dict) else 0
    delta.restore(delta.version() - 1)
    return int(deleted)
