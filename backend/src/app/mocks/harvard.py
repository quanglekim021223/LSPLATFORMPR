from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from datetime import UTC, datetime, time
from pathlib import PurePosixPath
from typing import Annotated, Any, Self
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from app.core.config import Settings
from app.mocks.settings import MockSettings, get_mock_settings
from app.models.harvard import RemoteFile

router = APIRouter(tags=["Harvard Catalog"])

def token_payload(token_value: str) -> dict[str, str | int]:
    return {"access_token": token_value, "expires_in": 3600}


def catalog_item(product_id: str, title: str) -> dict[str, str]:
    return {
        "ProductId": product_id,
        "AssetType": "Topic",
        "AssetFormat": "HTML",
        "Title": title,
        "Description": f"Description for {title}",
        "URL": f"https://example.test/assets/{product_id}",
        "ImageURL": "https://example.test/catalog.png",
        "Authors": "Harvard Business Publishing",
        "Duration": "60",
        "Language": "ENG",
        "PublicationDate": "2026-01-01",
        "CopyrightHolder": "Harvard Business Publishing",
        "SubjectTags": "Leadership",
        "Status": "Active",
        "MajorDiscipline": "General Management",
        "Series": "",
        "Skills": "Leading",
        "LastModifiedDate": "2026-08-01",
    }


def history_csv(vendor: str, report_date: str = "2026-08-22") -> bytes:
    if vendor == "harvard_hmm":
        compact_date = report_date.replace("-", "")
        return (
            "EventDate,Username,FirstName,LastName,Email,EventName,Title,Product\n"
            f"{compact_date},mock-hmm@example.test,Mock,Learner,"
            "mock-hmm@example.test,Completed,Decision Making,186DM-HTM-ENG\n"
        ).encode()
    if vendor == "harvard_spark":
        return (
            "Event Date,Username,First Name,Last Name,Email,Role,Event Name,"
            "Title,Asset Type,Product ID,Skills,Duration,Registration Date\n"
            f"{report_date},mock-spark@example.test,Mock,Learner,"
            "mock-spark@example.test,Learner,Views,Course Title,Videos,"
            "PRODUCT-1,Leadership,4,2026-01-21\n"
        ).encode()
    raise ValueError(f"Unsupported Harvard vendor: {vendor}")


_HMM_CATALOG = [
    catalog_item("hmm-1", "HMM Course 1"),
    catalog_item("hmm-2", "HMM Course 2"),
    catalog_item("hmm-3", "HMM Course 3"),
]
_SPARK_CATALOG = [
    catalog_item("spark-1", "Spark Course 1"),
    catalog_item("spark-2", "Spark Course 2"),
]


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

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.calls: list[str] = []

    async def __aenter__(self) -> Self:
        self._validate_connection(get_mock_settings())
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
            "harvard_hmm_reporting_": "hmm",
            "harvard_Spark_reporting_": "spark",
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
        prefix, vendor = matched
        date_value = file_name.removeprefix(prefix).removesuffix(".csv")
        try:
            report_date = datetime.strptime(date_value, "%Y%m%d").date()
        except ValueError:
            return None
        content = history_csv(f"harvard_{vendor}", report_date.isoformat())
        return RemoteFile(
            remote_path=remote_path,
            file_name=file_name,
            content=content,
            size=len(content),
            modified_at=datetime.combine(report_date, time(hour=23), UTC),
        )

    def _validate_connection(self, mock: MockSettings) -> None:
        if self.settings.harvard_sftp_host != mock.mock_harvard_sftp_host:
            raise ConnectionError("Unknown mock Harvard SFTP host")
        if (
            self.settings.harvard_sftp_username.get_secret_value()
            != mock.mock_harvard_sftp_username.get_secret_value()
            or self.settings.harvard_sftp_password.get_secret_value()
            != mock.mock_harvard_sftp_password.get_secret_value()
        ):
            raise PermissionError("Invalid mock Harvard SFTP credentials")
        known_hosts = self.settings.harvard_sftp_known_hosts
        if known_hosts is None or not known_hosts.is_file():
            raise ValueError("Mock Harvard SFTP known-hosts file is required")
        expected_entry = (
            f"{mock.mock_harvard_sftp_host} {mock.mock_harvard_sftp_host_key}"
        )
        trusted_entries = {
            line.strip()
            for line in known_hosts.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        if expected_entry not in trusted_entries:
            raise ValueError("Mock Harvard SFTP host key is not trusted")


@router.post("/v1/api/oauth/v2/accesstoken")
async def token(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str | int]:
    settings = get_mock_settings()
    if not authorization or not authorization.startswith("Basic "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    try:
        credentials = base64.b64decode(authorization.removeprefix("Basic ")).decode()
        client_id, client_secret = credentials.split(":", 1)
    except (ValueError, UnicodeDecodeError, binascii.Error):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials"
        ) from None
    clients = {
        settings.mock_harvard_hmm_client_id.get_secret_value(): (
            settings.mock_harvard_hmm_client_secret.get_secret_value(),
            settings.mock_harvard_hmm_access_token.get_secret_value(),
        ),
        settings.mock_harvard_spark_client_id.get_secret_value(): (
            settings.mock_harvard_spark_client_secret.get_secret_value(),
            settings.mock_harvard_spark_access_token.get_secret_value(),
        ),
    }
    expected = clients.get(client_id)
    form = parse_qs((await request.body()).decode())
    if (
        expected is None
        or client_secret != expected[0]
        or form.get("grant_type") != ["client_credentials"]
        or form.get("scope") != ["hbp.org.api/catalog.read"]
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock credentials")
    return token_payload(expected[1])


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
    settings = get_mock_settings()
    orgs = {
        settings.mock_harvard_hmm_org_key: ("HMM", _HMM_CATALOG),
        settings.mock_harvard_spark_org_key: ("HBR_SPARK", _SPARK_CATALOG),
    }
    config = orgs.get(org_key)
    if config is None or catalogs != config[0]:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown mock catalog")
    expected_token = (
        settings.mock_harvard_hmm_access_token.get_secret_value()
        if catalogs == "HMM"
        else settings.mock_harvard_spark_access_token.get_secret_value()
    )
    if authorization != f"Bearer {expected_token}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid mock token")
    records = config[1]
    return {
        "count": len(records),
        "limit": limit,
        "list": records[start : start + limit],
        "start": start,
    }
