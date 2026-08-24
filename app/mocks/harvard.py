from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from datetime import UTC, datetime, time
from pathlib import PurePosixPath
from typing import Annotated, Any, Self
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from app.vendors.harvard.models import RemoteFile

router = APIRouter(tags=["Harvard Catalog"])

_CLIENTS = {
    "mock-hmm-client": ("mock-hmm-secret", "mock-hmm-token"),
    "mock-spark-client": ("mock-spark-secret", "mock-spark-token"),
}
_ORGS = {
    "mock-hmm-org": (
        "HMM",
        [
            {"id": "hmm-1", "title": "HMM Course 1"},
            {"id": "hmm-2", "title": "HMM Course 2"},
            {"id": "hmm-3", "title": "HMM Course 3"},
        ],
    ),
    "mock-spark-org": (
        "HBR_SPARK",
        [
            {"id": "spark-1", "title": "Spark Course 1"},
            {"id": "spark-2", "title": "Spark Course 2"},
        ],
    ),
}


class MockHarvardSFTPTransport:
    def __init__(
        self,
        files: Mapping[str, RemoteFile],
        *,
        available_after: int = 0,
    ) -> None:
        self.files = dict(files)
        self.available_after = available_after
        self.calls: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object | None,
    ) -> None:
        return None

    async def fetch(self, remote_path: str) -> RemoteFile | None:
        self.calls.append(remote_path)
        if len(self.calls) <= self.available_after:
            return None
        return self.files.get(remote_path)


class GeneratedMockHarvardSFTPTransport:
    """Generate deterministic Harvard CSV files for local scheduled runs."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object | None,
    ) -> None:
        return None

    async def fetch(self, remote_path: str) -> RemoteFile | None:
        self.calls.append(remote_path)
        file_name = PurePosixPath(remote_path).name
        prefixes = {
            "harvard_hmm_reporting_": ("hmm", "hmm-course-001"),
            "harvard_Spark_reporting_": ("spark", "spark-course-001"),
        }
        matched = next(
            (
                (prefix, values)
                for prefix, values in prefixes.items()
                if file_name.startswith(prefix) and file_name.endswith(".csv")
            ),
            None,
        )
        if matched is None:
            return None
        prefix, (vendor, course_id) = matched
        date_value = file_name.removeprefix(prefix).removesuffix(".csv")
        try:
            report_date = datetime.strptime(date_value, "%Y%m%d").date()
        except ValueError:
            return None
        content = (
            "employee_id,course_id,status,report_date\n"
            f"mock-{vendor}-learner,{course_id},COMPLETED,{report_date.isoformat()}\n"
        ).encode()
        return RemoteFile(
            remote_path=remote_path,
            file_name=file_name,
            content=content,
            size=len(content),
            modified_at=datetime.combine(report_date, time(hour=23), UTC),
        )


@router.post("/v1/api/oauth/v2/accesstoken")
async def token(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str | int]:
    if not authorization or not authorization.startswith("Basic "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    try:
        credentials = base64.b64decode(authorization.removeprefix("Basic ")).decode()
        client_id, client_secret = credentials.split(":", 1)
    except (ValueError, UnicodeDecodeError, binascii.Error):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials"
        ) from None
    expected = _CLIENTS.get(client_id)
    form = parse_qs((await request.body()).decode())
    if (
        expected is None
        or client_secret != expected[0]
        or form.get("grant_type") != ["client_credentials"]
        or form.get("scope") != ["hbp.org.api/catalog.read"]
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return {"access_token": expected[1], "expires_in": 3600}


@router.get("/v1/api/catalog/{org_key}")
async def catalog(
    org_key: str,
    catalogs: Annotated[str, Query()],
    start: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 1000,
    start_date: Annotated[str | None, Query(alias="startDate")] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    del start_date
    config = _ORGS.get(org_key)
    if config is None or catalogs != config[0]:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock catalog")
    expected_token = (
        "mock-hmm-token" if catalogs == "HMM" else "mock-spark-token"
    )
    if authorization != f"Bearer {expected_token}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")
    records = config[1]
    return {"count": len(records), "list": records[start : start + limit]}
