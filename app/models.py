from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"


class RunSummary(BaseModel):
    run_id: str
    vendor: str = "levelup"
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    course_catalog_records: int = 0
    enrollment_records: int = 0
    courses_succeeded: int = 0
    courses_failed: int = 0
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class PageWrite:
    vendor: str
    data_domain: str
    ingestion_date: str
    run_id: str
    offset: int
    raw_payload: bytes
    records_count: int
    request_parameters: dict[str, Any]
    fetched_at: datetime
    course_id: str | None = None


@dataclass(slots=True)
class CourseResult:
    course_id: str
    records_count: int = 0
    succeeded: bool = True
    error_message: str | None = None



