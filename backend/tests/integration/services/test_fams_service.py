from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from app.main import create_app
from app.models import RunStatus
from app.repositories import CheckpointStore
from app.services.fams.service import run_fams_ingestion
from tests.conftest import no_sleep, response

TEST_ADMIN_USERNAME = "test-admin"
TEST_ADMIN_PASSWORD = "test-admin-password"


def _valid_payload() -> dict[str, Any]:
    return {
        "success": True,
        "message": "ok",
        "error_code": "",
        "data": {
            "classList": [{"classId": "c1"}, {"classId": "c2"}],
            "studentList": [
                {"studentId": "s1"},
                {"studentId": "s2"},
                {"studentId": "s3"},
            ],
        },
    }


@pytest.mark.asyncio
async def test_full_load_header_counts_and_exact_raw_bronze(
    settings_factory: Callable[..., object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = settings_factory()
    caplog.set_level(logging.DEBUG, logger="app.services.fams.training_data")
    raw_payload = (
        b'{\n  "success": true, "message": "ok", "error_code": "",\n'
        b'  "data": {"classList": [{"classId": "c1"}], '
        b'"studentList": [{"studentId": "s1"}, {"studentId": "s2"}]}\n}\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/fsa-reports/training-data"
        assert not request.url.params
        assert request.headers["Fsa-Report-Api-Key"] == "test-fams-key"
        assert request.headers["Accept"] == "application/json"
        return httpx.Response(
            200,
            content=raw_payload,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert summary.vendor == "fams"
    assert summary.records_by_domain == {"training_data": 3}
    raw_file = next(
        settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
    )
    assert raw_file.read_bytes() == raw_payload
    manifest = raw_file.with_name("manifest.json").read_text(encoding="utf-8")
    assert "test-fams-key" not in manifest
    assert "class_count=1 student_count=2" in caplog.text
    assert "test-fams-key" not in caplog.text


@pytest.mark.asyncio
async def test_filtered_load_sends_only_configured_filters(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        fams_load_mode="filtered",
        fams_status="CLOSED,INPROGRESS",
        fams_site="HCM",
        fams_actual_start_date_from="20260801",
        fams_actual_start_date_to="20260823",
    )
    observed_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed_params.update(request.url.params)
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert observed_params == {
        "status": "CLOSED,INPROGRESS",
        "site": "HCM",
        "actualStartDateFrom": "20260801",
        "actualStartDateTo": "20260823",
    }
    raw_files = list(
        settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
    )
    assert len(raw_files) == 1
    manifest = json.loads(raw_files[0].with_name("manifest.json").read_text())
    assert manifest["pages"][0]["request_parameters"] == observed_params


@pytest.mark.asyncio
async def test_full_mode_ignores_all_filter_validation_and_sends_no_params(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(
        fams_load_mode="full",
        fams_status="NOT_A_FAMS_STATUS",
        fams_site="HN,,HCM",
        fams_actual_start_date_from="not-a-date",
        fams_actual_start_date_to="also-invalid",
    )
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert not request.url.params
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert calls == 1


@pytest.mark.asyncio
async def test_filtered_mode_without_filters_fails_before_http_request(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory(fams_load_mode="filtered")
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert calls == 0
    assert summary.error_message is not None
    assert "requires at least one non-empty filter" in summary.error_message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        ({"fams_status": "UNKNOWN"}, "FAMS_STATUS"),
        ({"fams_site": "HN,,HCM"}, "FAMS_SITE"),
        ({"fams_actual_start_date_from": "2026-01-01"}, "YYYYMMDD"),
        (
            {
                "fams_actual_start_date_from": "20260824",
                "fams_actual_start_date_to": "20260823",
            },
            "must not be after",
        ),
    ],
)
async def test_filtered_configuration_errors_are_saved_in_latest_run(
    settings_factory: Callable[..., object],
    overrides: dict[str, str],
    expected_error: str,
) -> None:
    settings = settings_factory(fams_load_mode="filtered", **overrides)
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert calls == 0
    app = create_app(settings, checkpoint_store=store)  # type: ignore[arg-type]
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            login = await client.post(
                "/auth/login",
                json={
                    "userid": TEST_ADMIN_USERNAME,
                    "password": TEST_ADMIN_PASSWORD,
                },
            )
            client.headers["Authorization"] = (
                f"Bearer {login.json()['access_token']}"
            )
            latest = await client.get("/jobs/fams/latest")
    assert latest.status_code == 200
    assert latest.json()["run_id"] == summary.run_id
    assert latest.json()["status"] == "failed"
    assert expected_error in latest.json()["error_message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "success": False,
            "message": "request failed",
            "error_code": "FAMS_ERROR",
            "data": {"classList": [], "studentList": []},
        },
        {
            "success": True,
            "message": "ok",
            "error_code": "",
            "data": {"classList": {}, "studentList": []},
        },
        {
            "success": True,
            "message": "ok",
            "error_code": "",
            "data": {"classList": [], "studentList": None},
        },
    ],
)
async def test_invalid_contract_fails_without_writing_bronze(
    settings_factory: Callable[..., object],
    payload: dict[str, object],
) -> None:
    settings = settings_factory()
    raw_payload = json.dumps(payload, separators=(",", ":")).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=raw_payload,
            headers={"Content-Type": "application/json"},
            request=request,
        )

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert summary.records_by_domain == {}
    assert not list(
        settings.bronze_local_path.rglob("offset=000001.json")  # type: ignore[attr-defined]
    )


@pytest.mark.asyncio
async def test_unchanged_full_response_is_not_written_twice(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    reordered_payload = _valid_payload()
    reordered_payload["message"] = "same data, different envelope and order"
    reordered_payload["error_code"] = None
    reordered_payload["data"]["classList"].reverse()
    reordered_payload["data"]["studentList"].reverse()
    payloads = [
        _valid_payload(),
        reordered_payload,
    ]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        payload = payloads[calls]
        calls += 1
        return response(request, 200, payload)

    first = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    second = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert first.status == RunStatus.SUCCEEDED
    assert first.records_by_domain == {"training_data": 5}
    assert second.status == RunStatus.SUCCEEDED
    assert second.records_by_domain == {"training_data": 0}
    assert calls == 2
    assert len(
        list(
            settings.bronze_local_path.rglob(  # type: ignore[attr-defined]
                "offset=000001.json"
            )
        )
    ) == 1


@pytest.mark.asyncio
async def test_changed_full_response_creates_new_bronze_snapshot(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        payload = _valid_payload()
        if calls:
            payload["data"]["classList"][0]["updateDate"] = "2026-08-27"
        calls += 1
        return response(request, 200, payload)

    first = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    second = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        checkpoint_store=store,
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert first.status == RunStatus.SUCCEEDED
    assert second.status == RunStatus.SUCCEEDED
    assert second.records_by_domain == {"training_data": 5}
    assert len(
        list(
            settings.bronze_local_path.rglob(  # type: ignore[attr-defined]
                "offset=000001.json"
            )
        )
    ) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_retryable_http_status_is_retried(
    settings_factory: Callable[..., object],
    status_code: int,
) -> None:
    settings = settings_factory(http_max_retries=3)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return response(request, status_code, {"error": "temporary"})
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_api_key_and_allowlist_errors_are_not_retried(
    settings_factory: Callable[..., object],
    status_code: int,
) -> None:
    settings = settings_factory(http_max_retries=3)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return response(request, status_code, {"error": "denied"})

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.FAILED
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
async def test_network_and_timeout_errors_are_retried(
    settings_factory: Callable[..., object],
    error_type: type[httpx.RequestError],
) -> None:
    settings = settings_factory(http_max_retries=2)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error_type("temporary transport failure", request=request)
        return response(request, 200, _valid_payload())

    summary = await run_fams_ingestion(
        settings,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )

    assert summary.status == RunStatus.SUCCEEDED
    assert calls == 2
