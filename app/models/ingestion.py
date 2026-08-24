from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


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
    records_by_domain: dict[str, int] = Field(default_factory=dict)
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


@dataclass(frozen=True, slots=True)
class BinaryFileWrite:
    vendor: str
    data_domain: str
    ingestion_date: str
    run_id: str
    raw_payload: bytes
    file_name: str
    remote_path: str
    file_size: int
    remote_modified_time: datetime
    downloaded_at: datetime
    records_count: int = 0


@dataclass(slots=True)
class CourseResult:
    course_id: str
    records_count: int = 0
    succeeded: bool = True
    retryable: bool = False
    error_message: str | None = None
