"""Shared Bronze bootstrap table and record-path contract."""

from __future__ import annotations

import csv
import io
import json
from typing import Any, cast

# vendor, domain -> (endpoint ID, ((table, record path), ...))
DATASETS = {
    ("levelup", "course_catalog"): ("ABS-01", (("levelup_course_catalog", "courses"),)),
    ("levelup", "learning_history"): ("ABS-02", (("levelup_learning_history", "enrollments"),)),
    ("skillup", "skill_taxonomy"): ("IMO-01", (("skillup_skill_taxonomy", "items"),)),
    ("skillup", "skill_inventory"): ("IMO-02", (("skillup_skill_inventory", "items"),)),
    ("skillup", "assessment_history"): ("IMO-03", (("skillup_assessment_history", "reports"),)),
    ("skillup", "learning_resources"): (
        "IMO-04",
        (("skillup_learning_resources", "items"),),
    ),
    ("skillup", "certificates"): ("IMO-05", (("skillup_certificates", "items"),)),
    ("datacamp", "course_catalog_live"): ("DCP-01", (("datacamp_course_catalog_live", "data"),)),
    ("datacamp", "course_catalog_archived"): (
        "DCP-02",
        (("datacamp_course_catalog_archived", "data"),),
    ),
    ("datacamp", "learning_history"): ("DCP-03", (("datacamp_learning_history", "data"),)),
    ("coursera", "course_catalog"): ("COU-01", (("coursera_course_catalog", "elements"),)),
    ("coursera", "course_detail"): ("COU-02", (("coursera_course_detail", "elements"),)),
    ("coursera", "learning_history"): ("COU-03", (("coursera_learning_history", "elements"),)),
    ("linkedin", "course_catalog"): ("LNK-01", (("linkedin_course_catalog", "elements"),)),
    ("linkedin", "learning_history"): ("LNK-02", (("linkedin_learning_history", "elements"),)),
    ("harvard_hmm", "course_catalog"): ("HMM-01", (("harvard_hmm_course_catalog", "list"),)),
    ("harvard_hmm", "learning_history"): ("HMM-03", (("harvard_hmm_learning_history", "csv"),)),
    ("harvard_spark", "course_catalog"): ("SPK-01", (("harvard_spark_course_catalog", "list"),)),
    ("harvard_spark", "learning_history"): ("SPK-03", (("harvard_spark_learning_history", "csv"),)),
    ("fams", "training_data"): (
        "FAM-01",
        (
            ("fams_training_classes", "data.classList"),
            ("fams_training_students", "data.studentList"),
        ),
    ),
}
TABLES = {table for _, mappings in DATASETS.values() for table, _ in mappings}

# Source-owned fields used to identify one current business record. History APIs
# without an event ID use the smallest stable composite exposed by their contract.
BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "levelup_course_catalog": ("id",),
    "levelup_learning_history": ("id",),
    "skillup_skill_taxonomy": ("taxonomyskillid",),
    "skillup_skill_inventory": ("employeeid",),
    "skillup_assessment_history": ("testinvitationid",),
    "skillup_learning_resources": ("learning_material_id",),
    "skillup_certificates": ("certificate_id",),
    "datacamp_course_catalog_live": ("id",),
    "datacamp_course_catalog_archived": ("id",),
    "datacamp_learning_history": ("user", "contentid", "eventtype", "timestamp"),
    "coursera_course_catalog": ("contentid",),
    "coursera_course_detail": ("contentid",),
    "coursera_learning_history": ("id",),
    "linkedin_course_catalog": ("urn",),
    "linkedin_learning_history": ("learnerdetails", "contentdetails"),
    "harvard_hmm_course_catalog": ("productid",),
    "harvard_hmm_learning_history": ("eventdate", "username", "eventname", "product_id"),
    "harvard_spark_course_catalog": ("productid",),
    "harvard_spark_learning_history": (
        "event_date",
        "username",
        "event_name",
        "product_id",
    ),
    "fams_training_classes": ("id",),
    "fams_training_students": ("coursecode", "account", "email"),
}


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _repair_csv_row(headers: list[str], row: list[str]) -> list[str]:
    if len(row) == len(headers):
        return row
    title_index = headers.index("Title") if "Title" in headers else -1
    if len(row) <= len(headers) or title_index < 0:
        raise ValueError("CSV column count mismatch")
    title_end = len(row) - (len(headers) - title_index - 1)
    return row[:title_index] + [",".join(row[title_index:title_end])] + row[title_end:]


def _csv_records(raw: bytes) -> list[dict[str, Any]]:
    reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    headers = next(reader, None)
    if not headers or len(headers) != len(set(headers)):
        raise ValueError("Missing or duplicate CSV headers")
    return [dict(zip(headers, _repair_csv_row(headers, row), strict=True)) for row in reader]


def _json_records(raw: bytes, path: str) -> object:
    value = json.loads(raw)
    # Absorb live responses may be bare arrays; no synthetic envelope is persisted.
    if isinstance(value, list) and path in {"courses", "enrollments"}:
        return value
    for component in path.split("."):
        if not isinstance(value, dict) or component not in value:
            raise ValueError(f"Missing record path: {path}")
        value = value[component]
    return cast(object, value)


def records_from_bytes(raw: bytes, path: str) -> list[dict[str, Any]]:
    records = _csv_records(raw) if path == "csv" else _json_records(raw, path)
    if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
        raise ValueError("Expected an array of record objects")
    return records
