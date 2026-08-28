from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

import app.repositories.adls_writer as adls_module
from app.models import BinaryFileWrite, PageWrite


class FakeDownload:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def readall(self) -> bytes:
        return self.payload


class FakeFileClient:
    def __init__(self, file_system: FakeFileSystemClient, path: str) -> None:
        self.file_system = file_system
        self.path = path

    @property
    def url(self) -> str:
        return f"https://storage.test/{self.file_system.name}/{self.path}"

    def upload_data(self, payload: bytes, *, overwrite: bool) -> None:
        if self.path in self.file_system.files and not overwrite:
            raise ResourceExistsError("file already exists")
        self.file_system.files[self.path] = payload

    def exists(self) -> bool:
        return self.path in self.file_system.files

    def download_file(self) -> FakeDownload:
        try:
            return FakeDownload(self.file_system.files[self.path])
        except KeyError as exc:
            raise ResourceNotFoundError("file not found") from exc

    def delete_file(self) -> None:
        try:
            del self.file_system.files[self.path]
        except KeyError as exc:
            raise ResourceNotFoundError("file not found") from exc

    def rename_file(self, new_name: str) -> FakeFileClient:
        file_system, separator, destination = new_name.partition("/")
        assert separator and file_system == self.file_system.name
        self.file_system.files[destination] = self.file_system.files.pop(self.path)
        return FakeFileClient(self.file_system, destination)


class FakeDirectoryClient:
    def __init__(self, file_system: FakeFileSystemClient, path: str) -> None:
        self.file_system = file_system
        self.path = path

    def create_directory(self) -> None:
        if self.path in self.file_system.directories:
            raise ResourceExistsError("directory already exists")
        self.file_system.directories.add(self.path)


class FakeFileSystemClient:
    def __init__(self, name: str) -> None:
        self.name = name
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = set()

    def get_file_client(self, path: str) -> FakeFileClient:
        return FakeFileClient(self, path)

    def get_directory_client(self, path: str) -> FakeDirectoryClient:
        return FakeDirectoryClient(self, path)


class FakeServiceClient:
    def __init__(self, file_system: FakeFileSystemClient) -> None:
        self.file_system = file_system

    def get_file_system_client(self, name: str) -> FakeFileSystemClient:
        assert name == self.file_system.name
        return self.file_system


@pytest.fixture
def adls_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[adls_module.ADLSGen2BronzeWriter, FakeFileSystemClient, dict[str, Any]]:
    file_system = FakeFileSystemClient("bronze")
    service = FakeServiceClient(file_system)
    constructor: dict[str, Any] = {}

    def service_client(**kwargs: Any) -> FakeServiceClient:
        constructor.update(kwargs)
        return service

    credential = object()
    monkeypatch.setattr(adls_module, "DefaultAzureCredential", lambda: credential)
    monkeypatch.setattr(adls_module, "DataLakeServiceClient", service_client)
    writer = adls_module.ADLSGen2BronzeWriter(
        account_name="fsastorage",
        file_system="bronze",
        base_path="raw",
    )
    return writer, file_system, constructor


async def test_adls_writer_uploads_temp_then_renames_and_merges_manifest(
    adls_writer: tuple[
        adls_module.ADLSGen2BronzeWriter,
        FakeFileSystemClient,
        dict[str, Any],
    ],
) -> None:
    writer, file_system, constructor = adls_writer
    fetched_at = datetime(2026, 8, 28, 5, 0, tzinfo=UTC)

    first = await writer.write_page(
        PageWrite(
            vendor="levelup",
            data_domain="learning_history",
            ingestion_date="2026-08-28",
            run_id="run-1",
            course_id="course/unsafe",
            offset=0,
            raw_payload=b'{"id":"first"}',
            records_count=1,
            request_parameters={"Authorization": "secret"},
            fetched_at=fetched_at,
        )
    )
    await writer.write_page(
        PageWrite(
            vendor="levelup",
            data_domain="learning_history",
            ingestion_date="2026-08-28",
            run_id="run-1",
            course_id="course/unsafe",
            offset=100,
            raw_payload=b'{"id":"second"}',
            records_count=2,
            request_parameters={"_offset": 100},
            fetched_at=fetched_at,
        )
    )

    directory = (
        "raw/levelup/learning_history/ingestion_date=2026-08-28/"
        "run_id=run-1/course_id=course%2Funsafe"
    )
    first_path = f"{directory}/offset=000000.json"
    manifest = json.loads(file_system.files[f"{directory}/manifest.json"])

    assert constructor["account_url"] == "https://fsastorage.dfs.core.windows.net"
    assert file_system.files[first_path] == b'{"id":"first"}'
    assert not any(".tmp-" in path for path in file_system.files)
    assert [page["offset"] for page in manifest["pages"]] == [0, 100]
    assert manifest["records_count"] == 3
    assert manifest["pages"][0]["request_parameters"]["Authorization"] == "[REDACTED]"
    assert first.uri == f"https://storage.test/bronze/{first_path}"
    assert first.size_bytes == len(b'{"id":"first"}')
    assert first.sha256 == hashlib.sha256(b'{"id":"first"}').hexdigest()


async def test_adls_binary_writer_preserves_csv_and_manifest(
    adls_writer: tuple[
        adls_module.ADLSGen2BronzeWriter,
        FakeFileSystemClient,
        dict[str, Any],
    ],
) -> None:
    writer, file_system, _constructor = adls_writer
    raw = b"learner_id,course_id\n1,c1\n"
    timestamp = datetime(2026, 8, 28, 5, 0, tzinfo=UTC)

    result = await writer.write_file(
        BinaryFileWrite(
            vendor="harvard_hmm",
            data_domain="learning_history",
            ingestion_date="2026-08-28",
            run_id="run-2",
            raw_payload=raw,
            file_name="harvard_hmm_reporting_20260828.csv",
            remote_path="/reports/harvard_hmm_reporting_20260828.csv",
            file_size=len(raw),
            remote_modified_time=timestamp,
            downloaded_at=timestamp,
            records_count=1,
        )
    )

    directory = (
        "raw/harvard_hmm/learning_history/ingestion_date=2026-08-28/"
        "run_id=run-2"
    )
    output_path = f"{directory}/harvard_hmm_reporting_20260828.csv"
    manifest = json.loads(file_system.files[f"{directory}/manifest.json"])

    assert file_system.files[output_path] == raw
    assert manifest["remote_path"] == "/reports/harvard_hmm_reporting_20260828.csv"
    assert manifest["sha256"] == hashlib.sha256(raw).hexdigest()
    assert result.uri == f"https://storage.test/bronze/{output_path}"


def test_adls_writer_rejects_relative_base_path() -> None:
    with pytest.raises(ValueError, match="relative path"):
        adls_module.ADLSGen2BronzeWriter("fsastorage", "bronze", "../raw")
