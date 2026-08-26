from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from app.clients.harvard_sftp_client import AsyncSSHSFTPTransport
from app.core.config import Settings
from app.mocks.harvard import (
    GeneratedMockHarvardSFTPTransport,
    MockHarvardSFTPTransport,
    catalog_item,
    history_csv,
    token_payload,
)
from app.mocks.settings import get_mock_settings
from app.models import RunStatus
from app.models.harvard import RemoteFile
from app.services.harvard.hmm_service import run_harvard_hmm_ingestion
from app.services.harvard.spark_service import run_harvard_spark_ingestion
from tests.conftest import no_sleep, response

NOW = datetime(2026, 8, 23, 5, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))


def _empty_catalog() -> dict[str, object]:
    return {"count": 0, "limit": 2, "list": [], "start": 0}


@pytest.mark.parametrize(
    ("vendor", "org_key", "catalog_code", "prefix", "runner"),
    [
        (
            "harvard_hmm",
            "test-hmm-org",
            "HMM",
            "harvard_hmm_reporting_",
            run_harvard_hmm_ingestion,
        ),
        (
            "harvard_spark",
            "test-spark-org",
            "HBR_SPARK",
            "harvard_Spark_reporting_",
            run_harvard_spark_ingestion,
        ),
    ],
)
@pytest.mark.asyncio
async def test_full_pipeline_paginates_and_preserves_catalog_and_csv(
    settings_factory: Callable[..., Settings],
    vendor: str,
    org_key: str,
    catalog_code: str,
    prefix: str,
    runner: Callable[..., Awaitable[Any]],
) -> None:
    settings = settings_factory()
    starts: list[int] = []
    first_page = {
        "count": 3,
        "limit": 2,
        "list": [
            catalog_item("one", "Course One"),
            catalog_item("two", "Course Two"),
        ],
        "start": 0,
    }
    raw_first_page = json.dumps(first_page, indent=2).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("catalog-token"))
        assert request.url.path == f"/v1/api/catalog/{org_key}"
        assert request.headers["Authorization"] == "Bearer catalog-token"
        assert request.url.params["catalogs"] == catalog_code
        assert "startDate" not in request.url.params
        start = int(request.url.params["start"])
        starts.append(start)
        if start == 0:
            return httpx.Response(
                200,
                content=raw_first_page,
                headers={"Content-Type": "application/json"},
                request=request,
            )
        return response(
            request,
            200,
            {
                "count": 3,
                "limit": 2,
                "list": [catalog_item("three", "Course Three")],
                "start": 2,
            },
        )

    file_name = f"{prefix}20260822.csv"
    remote_path = f"/reports/{file_name}"
    raw_csv = history_csv(vendor, "2026-08-22")
    sftp = MockHarvardSFTPTransport(
        {
            remote_path: RemoteFile(
                remote_path=remote_path,
                file_name=file_name,
                content=raw_csv,
                size=len(raw_csv),
                modified_at=datetime(2026, 8, 22, 23, 0, tzinfo=UTC),
            )
        },
        available_after=2,
    )

    summary = await runner(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.vendor == vendor
    assert summary.status == RunStatus.SUCCEEDED
    assert summary.records_by_domain == {
        "course_catalog": 3,
        "learning_history": 1,
    }
    assert starts == [0, 2]
    assert sftp.calls == [remote_path, remote_path, remote_path]
    catalog_page = next(
        settings.bronze_local_path.glob(
            f"{vendor}/course_catalog/**/offset=000000.json"
        )
    )
    assert catalog_page.read_bytes() == raw_first_page
    catalog_manifest_text = (catalog_page.parent / "manifest.json").read_text()
    assert "catalog-token" not in catalog_manifest_text
    assert "test-hmm-secret" not in catalog_manifest_text
    assert "test-spark-secret" not in catalog_manifest_text
    csv_path = next(
        settings.bronze_local_path.glob(f"{vendor}/learning_history/**/{file_name}")
    )
    assert csv_path.read_bytes() == raw_csv
    manifest = json.loads((csv_path.parent / "manifest.json").read_text())
    assert manifest["remote_path"] == remote_path
    assert manifest["file_size"] == len(raw_csv)
    assert manifest["sha256"] == hashlib.sha256(raw_csv).hexdigest()
    assert manifest["run_id"] == summary.run_id
    assert manifest["ingestion_date"] == "2026-08-23"


@pytest.mark.parametrize(
    ("vendor", "prefix", "runner"),
    [
        (
            "harvard_hmm",
            "harvard_hmm_reporting_",
            run_harvard_hmm_ingestion,
        ),
        (
            "harvard_spark",
            "harvard_Spark_reporting_",
            run_harvard_spark_ingestion,
        ),
    ],
)
@pytest.mark.asyncio
async def test_local_mock_mode_validates_sftp_credentials_and_host_key(
    settings_factory: Callable[..., Settings],
    tmp_path: Path,
    vendor: str,
    prefix: str,
    runner: Callable[..., Awaitable[Any]],
) -> None:
    mock = get_mock_settings()
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text(
        f"{mock.mock_harvard_sftp_host} {mock.mock_harvard_sftp_host_key}\n",
        encoding="utf-8",
    )
    settings = settings_factory(
        harvard_sftp_mock_enabled=True,
        harvard_sftp_host=mock.mock_harvard_sftp_host,
        harvard_sftp_username=mock.mock_harvard_sftp_username.get_secret_value(),
        harvard_sftp_password=mock.mock_harvard_sftp_password.get_secret_value(),
        harvard_sftp_known_hosts=known_hosts,
    )
    assert (
        settings.harvard_hmm_configured
        if vendor == "harvard_hmm"
        else settings.harvard_spark_configured
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("mock-token"))
        return response(request, 200, _empty_catalog())

    summary = await runner(
        settings,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.status == RunStatus.SUCCEEDED
    csv_path = next(
        settings.bronze_local_path.glob(
            f"{vendor}/learning_history/**/{prefix}20260822.csv"
        )
    )
    assert b"mock-" in csv_path.read_bytes()


@pytest.mark.asyncio
async def test_local_sftp_mock_rejects_wrong_password_and_untrusted_host(
    settings_factory: Callable[..., Settings],
    tmp_path: Path,
) -> None:
    mock = get_mock_settings()
    untrusted = tmp_path / "known_hosts"
    untrusted.write_text("other-host ssh-ed25519 other-key\n", encoding="utf-8")
    wrong_password = settings_factory(
        harvard_sftp_mock_enabled=True,
        harvard_sftp_host=mock.mock_harvard_sftp_host,
        harvard_sftp_username=mock.mock_harvard_sftp_username.get_secret_value(),
        harvard_sftp_password="wrong",
        harvard_sftp_known_hosts=untrusted,
    )
    with pytest.raises(PermissionError, match="credentials"):
        async with GeneratedMockHarvardSFTPTransport(wrong_password):
            pass

    valid_password = settings_factory(
        harvard_sftp_mock_enabled=True,
        harvard_sftp_host=mock.mock_harvard_sftp_host,
        harvard_sftp_username=mock.mock_harvard_sftp_username.get_secret_value(),
        harvard_sftp_password=mock.mock_harvard_sftp_password.get_secret_value(),
        harvard_sftp_known_hosts=untrusted,
    )
    with pytest.raises(ValueError, match="host key is not trusted"):
        async with GeneratedMockHarvardSFTPTransport(valid_password):
            pass


@pytest.mark.asyncio
async def test_history_backfills_once_then_only_downloads_new_date(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(harvard_hmm_history_start_date="2026-08-20")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        return response(request, 200, _empty_catalog())

    def remote(report_date: str) -> RemoteFile:
        name = f"harvard_hmm_reporting_{report_date}.csv"
        path = f"/reports/{name}"
        content = history_csv(
            "harvard_hmm",
            datetime.strptime(report_date, "%Y%m%d").date().isoformat(),
        )
        return RemoteFile(
            remote_path=path,
            file_name=name,
            content=content,
            size=len(content),
            modified_at=datetime(2026, 8, 22, tzinfo=UTC),
        )

    initial_files = {
        item.remote_path: item
        for item in (
            remote("20260820"),
            remote("20260821"),
            remote("20260822"),
        )
    }
    first_sftp = MockHarvardSFTPTransport(initial_files)
    first = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=first_sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert first.status == RunStatus.SUCCEEDED
    assert first_sftp.calls == sorted(initial_files)
    first_manifest_path = next(
        settings.bronze_local_path.glob(
            f"harvard_hmm/learning_history/**/run_id={first.run_id}/manifest.json"
        )
    )
    first_manifest = json.loads(first_manifest_path.read_text())
    assert [item["remote_filename"] for item in first_manifest["files"]] == [
        "harvard_hmm_reporting_20260820.csv",
        "harvard_hmm_reporting_20260821.csv",
        "harvard_hmm_reporting_20260822.csv",
    ]

    already_complete_sftp = MockHarvardSFTPTransport({})
    second = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=already_complete_sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )
    assert second.status == RunStatus.SUCCEEDED
    assert second.run_id != first.run_id
    assert already_complete_sftp.calls == []

    next_file = remote("20260823")
    next_sftp = MockHarvardSFTPTransport({next_file.remote_path: next_file})
    third = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=next_sftp,
        sleep=no_sleep,
        now=lambda: NOW + timedelta(days=1),
    )
    assert third.status == RunStatus.SUCCEEDED
    assert next_sftp.calls == [next_file.remote_path]


@pytest.mark.asyncio
async def test_backfill_keeps_successful_files_and_retries_only_missing_file(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(
        harvard_hmm_history_start_date="2026-08-20",
        harvard_sftp_max_wait_seconds=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        return response(request, 200, _empty_catalog())

    def remote(report_date: str) -> RemoteFile:
        name = f"harvard_hmm_reporting_{report_date}.csv"
        path = f"/reports/{name}"
        content = history_csv(
            "harvard_hmm",
            datetime.strptime(report_date, "%Y%m%d").date().isoformat(),
        )
        return RemoteFile(
            remote_path=path,
            file_name=name,
            content=content,
            size=len(content),
            modified_at=datetime(2026, 8, 22, tzinfo=UTC),
        )

    day_20 = remote("20260820")
    day_22 = remote("20260822")
    first_sftp = MockHarvardSFTPTransport(
        {day_20.remote_path: day_20, day_22.remote_path: day_22}
    )
    first = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=first_sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )
    assert first.status == RunStatus.PARTIAL_FAILURE
    assert len(
        list(
            settings.bronze_local_path.glob(
                f"harvard_hmm/learning_history/**/run_id={first.run_id}/*.csv"
            )
        )
    ) == 2

    day_21 = remote("20260821")
    retry_sftp = MockHarvardSFTPTransport({day_21.remote_path: day_21})
    retry = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=retry_sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )
    assert retry.status == RunStatus.SUCCEEDED
    assert retry_sftp.calls == [day_21.remote_path]


@pytest.mark.asyncio
async def test_start_date_is_only_sent_when_explicitly_requested(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        assert request.url.params["startDate"] == "20260801"
        return response(request, 200, _empty_catalog())

    file_name = "harvard_hmm_reporting_20260822.csv"
    sftp = MockHarvardSFTPTransport(
        {
            f"/reports/{file_name}": RemoteFile(
                remote_path=f"/reports/{file_name}",
                file_name=file_name,
                content=history_csv("harvard_hmm"),
                size=len(history_csv("harvard_hmm")),
                modified_at=datetime(2026, 8, 22, tzinfo=UTC),
            )
        }
    )
    summary = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=sftp,
        sleep=no_sleep,
        now=lambda: NOW,
        start_date="20260801",
    )
    assert summary.status == RunStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_invalid_catalog_contract_is_not_written_and_causes_partial_failure(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    invalid_raw = b'{\n  "count": 1, "list": {"unexpected": true}\n}'

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        return httpx.Response(
            200,
            content=invalid_raw,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    file_name = "harvard_hmm_reporting_20260822.csv"
    path = f"/reports/{file_name}"
    sftp = MockHarvardSFTPTransport(
        {
            path: RemoteFile(
                remote_path=path,
                file_name=file_name,
                content=history_csv("harvard_hmm"),
                size=len(history_csv("harvard_hmm")),
                modified_at=datetime(2026, 8, 22, tzinfo=UTC),
            )
        }
    )
    summary = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert not list(
        settings.bronze_local_path.glob(
            "harvard_hmm/course_catalog/**/offset=000000.json"
        )
    )


@pytest.mark.asyncio
async def test_invalid_history_contract_is_not_written_and_causes_partial_failure(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        return response(request, 200, _empty_catalog())

    file_name = "harvard_hmm_reporting_20260822.csv"
    path = f"/reports/{file_name}"
    invalid_csv = b"EventDate,Unexpected\n20260822,value\n"
    sftp = MockHarvardSFTPTransport(
        {
            path: RemoteFile(
                remote_path=path,
                file_name=file_name,
                content=invalid_csv,
                size=len(invalid_csv),
                modified_at=datetime(2026, 8, 22, tzinfo=UTC),
            )
        }
    )

    summary = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert not list(
        settings.bronze_local_path.glob(
            "harvard_hmm/learning_history/**/*.csv"
        )
    )


@pytest.mark.asyncio
async def test_missing_sftp_file_causes_partial_failure_and_redacts_secrets(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(harvard_sftp_max_wait_seconds=0)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return response(request, 200, token_payload("token"))
        return response(request, 200, _empty_catalog())

    class SecretFailureTransport:
        async def __aenter__(self) -> SecretFailureTransport:
            return self

        async def __aexit__(
            self,
            _exc_type: type[BaseException] | None,
            _exc: BaseException | None,
            _traceback: object | None,
        ) -> None:
            return None

        async def fetch(self, _remote_path: str) -> RemoteFile | None:
            raise RuntimeError(
                "failed with test-sftp-user test-sftp-password test-hmm-secret"
            )

    summary = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(handler),
        sftp_transport=SecretFailureTransport(),
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    with sqlite3.connect(settings.checkpoint_db_path) as connection:
        error = connection.execute(
            "SELECT error_message FROM checkpoints "
            "WHERE run_id = ? AND data_domain = 'learning_history'",
            (summary.run_id,),
        ).fetchone()[0]
    assert "[REDACTED]" in error
    assert "test-sftp-user" not in error
    assert "test-sftp-password" not in error
    assert "test-hmm-secret" not in error


@pytest.mark.asyncio
async def test_missing_catalog_configuration_only_fails_catalog_branch(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(harvard_hmm_client_id="")
    file_name = "harvard_hmm_reporting_20260822.csv"
    path = f"/reports/{file_name}"
    sftp = MockHarvardSFTPTransport(
        {
            path: RemoteFile(
                remote_path=path,
                file_name=file_name,
                content=history_csv("harvard_hmm"),
                size=len(history_csv("harvard_hmm")),
                modified_at=datetime(2026, 8, 22, tzinfo=UTC),
            )
        }
    )

    summary = await run_harvard_hmm_ingestion(
        settings,
        transport=httpx.MockTransport(
            lambda request: pytest.fail(f"Unexpected HTTP request: {request.url}")
        ),
        sftp_transport=sftp,
        sleep=no_sleep,
        now=lambda: NOW,
    )

    assert summary.status == RunStatus.PARTIAL_FAILURE
    assert list(
        settings.bronze_local_path.glob(
            "harvard_hmm/learning_history/**/*.csv"
        )
    )


def test_sftp_transport_requires_known_hosts(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(harvard_sftp_known_hosts=None)
    with pytest.raises(ValueError, match="host-key verification"):
        AsyncSSHSFTPTransport(settings)


@pytest.mark.asyncio
async def test_asyncssh_receives_the_configured_known_hosts_file(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory()
    connect_arguments: dict[str, Any] = {}
    connect_calls = 0

    class FakeRemoteHandle:
        async def __aenter__(self) -> FakeRemoteHandle:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def read(self) -> bytes:
            return b"raw-csv"

    class FakeSFTP:
        async def __aenter__(self) -> FakeSFTP:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def stat(self, _path: str) -> SimpleNamespace:
            return SimpleNamespace(size=7, mtime=1_777_000_000)

        def open(self, _path: str, _mode: str) -> FakeRemoteHandle:
            return FakeRemoteHandle()

    class FakeConnection:
        async def __aenter__(self) -> FakeConnection:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        def start_sftp_client(self) -> FakeSFTP:
            return FakeSFTP()

    def connect(host: str, **kwargs: Any) -> FakeConnection:
        nonlocal connect_calls
        connect_calls += 1
        connect_arguments.update({"host": host, **kwargs})
        return FakeConnection()

    fake_asyncssh = SimpleNamespace(
        connect=connect,
        SFTPNoSuchFile=type("SFTPNoSuchFile", (Exception,), {}),
    )
    monkeypatch.setitem(sys.modules, "asyncssh", fake_asyncssh)

    transport = AsyncSSHSFTPTransport(settings)
    async with transport:
        result = await transport.fetch("/reports/report.csv")
        second = await transport.fetch("/reports/second.csv")

    assert result is not None
    assert result.content == b"raw-csv"
    assert second is not None
    assert connect_calls == 1
    assert connect_arguments["host"] == "sftp.harvard.test"
    assert connect_arguments["known_hosts"] == str(settings.harvard_sftp_known_hosts)
    assert connect_arguments["known_hosts"] is not None


@pytest.mark.asyncio
async def test_sftp_connection_reset_reopens_session_and_retries(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_factory(harvard_sftp_max_retries=3)
    connect_calls = 0
    stat_calls = 0
    sleeps: list[float] = []

    class FakeRemoteHandle:
        async def __aenter__(self) -> FakeRemoteHandle:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def read(self) -> bytes:
            return b"recovered"

    class FlakySFTP:
        async def __aenter__(self) -> FlakySFTP:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def stat(self, _path: str) -> SimpleNamespace:
            nonlocal stat_calls
            stat_calls += 1
            if stat_calls <= 3:
                raise ConnectionResetError("temporary reset")
            return SimpleNamespace(size=9, mtime=1_777_000_000)

        def open(self, _path: str, _mode: str) -> FakeRemoteHandle:
            return FakeRemoteHandle()

    class FakeConnection:
        async def __aenter__(self) -> FakeConnection:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        def start_sftp_client(self) -> FlakySFTP:
            return FlakySFTP()

    def connect(_host: str, **_kwargs: Any) -> FakeConnection:
        nonlocal connect_calls
        connect_calls += 1
        return FakeConnection()

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    fake_asyncssh = SimpleNamespace(
        connect=connect,
        SFTPNoSuchFile=type("SFTPNoSuchFile", (Exception,), {}),
        ConnectionLost=type("ConnectionLost", (Exception,), {}),
        SFTPConnectionLost=type("SFTPConnectionLost", (Exception,), {}),
    )
    monkeypatch.setitem(sys.modules, "asyncssh", fake_asyncssh)
    transport = AsyncSSHSFTPTransport(
        settings, sleep=record_sleep, jitter=lambda: 0.0
    )

    result = await transport.fetch("/reports/report.csv")

    assert result is not None
    assert result.content == b"recovered"
    assert connect_calls == 4
    assert stat_calls == 4
    assert len(sleeps) == 3
