from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import function_app
from app.core.config import Settings
from app.main import build_ingestion_jobs
from app.models import RunStatus, RunSummary


def test_function_app_registers_asgi_timer_and_queue_triggers() -> None:
    functions = {
        function.get_function_name(): function for function in function_app.app.get_functions()
    }

    assert "http_app_func" in functions
    timer_binding = functions["scheduled_vendor_ingestion"].get_dict_repr()["bindings"][0]
    assert timer_binding["schedule"] == "%INGESTION_TIMER_SCHEDULE%"
    assert timer_binding["runOnStartup"] is False
    assert timer_binding["useMonitor"] is True
    queue_binding = functions["manual_vendor_ingestion"].get_dict_repr()["bindings"][0]
    assert queue_binding["queueName"] == "%INGESTION_QUEUE_NAME%"
    assert queue_binding["connection"] == "AzureWebJobsStorage"
    assert function_app.settings.scheduler_enabled is False


def test_timer_job_registry_contains_all_eight_vendors(
    settings_factory: Callable[..., Settings],
) -> None:
    jobs = build_ingestion_jobs(
        settings_factory(),
        function_app.checkpoint_store,
        function_app.bronze_writer,
    )

    assert set(jobs) == {
        "levelup",
        "skillup",
        "datacamp",
        "coursera",
        "linkedin",
        "harvard_hmm",
        "harvard_spark",
        "fams",
    }


async def test_fabric_job_registry_uses_selected_vendor_and_fabric_runner(
    monkeypatch: pytest.MonkeyPatch,
    settings_factory: Callable[..., Settings],
) -> None:
    fabric_run = AsyncMock(return_value=object())
    monkeypatch.setattr("app.fabric_job.run_fabric_ingestion", fabric_run)
    settings = settings_factory(
        fabric_enabled=True,
        fabric_workspace_id="851dacd8-2ad0-42a1-8817-55d2a7682bc6",
        fabric_lakehouse_id="3535e0e3-a2a0-4387-82e6-1e7d8cbd1a62",
        fabric_state_account_url="https://stateaccount.blob.core.windows.net",
        fabric_vendors=["levelup"],
    )

    jobs = build_ingestion_jobs(
        settings,
        function_app.checkpoint_store,
        function_app.bronze_writer,
    )

    assert set(jobs) == {"levelup"}
    await jobs["levelup"]()
    fabric_run.assert_awaited_once()
    call_settings, vendor, runner = fabric_run.await_args.args
    assert call_settings is settings
    assert vendor == "levelup"
    assert runner.__name__ == "run_levelup_ingestion"


async def test_timer_runs_every_configured_vendor_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed: list[str] = []
    initialize = AsyncMock()
    monkeypatch.setattr(function_app.checkpoint_store, "initialize", initialize)

    def job(vendor: str) -> Callable[[], Awaitable[object]]:
        async def run() -> object:
            completed.append(vendor)
            return object()

        return run

    monkeypatch.setattr(
        function_app,
        "build_ingestion_jobs",
        lambda *_args: {
            "levelup": job("levelup"),
            "fams": job("fams"),
        },
    )

    await function_app.run_configured_ingestions()

    initialize.assert_awaited_once()
    assert set(completed) == {"levelup", "fams"}


async def test_timer_waits_for_all_jobs_before_reporting_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed: list[str] = []
    monkeypatch.setattr(
        function_app.checkpoint_store,
        "initialize",
        AsyncMock(),
    )

    async def failed_job() -> object:
        raise RuntimeError("upstream unavailable")

    async def successful_job() -> object:
        completed.append("fams")
        return object()

    monkeypatch.setattr(
        function_app,
        "build_ingestion_jobs",
        lambda *_args: {
            "levelup": failed_job,
            "fams": successful_job,
        },
    )

    with pytest.raises(RuntimeError, match="levelup"):
        await function_app.run_configured_ingestions()

    assert completed == ["fams"]


async def test_timer_logs_when_invocation_is_past_due(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    run_ingestions = AsyncMock()
    monkeypatch.setattr(function_app, "run_configured_ingestions", run_ingestions)

    with caplog.at_level("WARNING"):
        await function_app.scheduled_vendor_ingestion(SimpleNamespace(past_due=True))

    run_ingestions.assert_awaited_once()
    assert "Azure ingestion timer is past due" in caplog.text


async def test_queue_trigger_runs_persisted_job(monkeypatch: pytest.MonkeyPatch) -> None:
    run_queued = AsyncMock()
    monkeypatch.setattr(function_app.ingestion_coordinator, "run_queued", run_queued)
    message = SimpleNamespace(
        get_body=lambda: b'{"job_id":"job-1","vendors":["skillup"]}'
    )

    await function_app.manual_vendor_ingestion(message)

    queued = run_queued.await_args.args[0]
    assert queued.job_id == "job-1"
    assert queued.vendors == ["skillup"]


@pytest.mark.parametrize("status", [RunStatus.PARTIAL_FAILURE, RunStatus.FAILED])
async def test_timer_rejects_unsuccessful_summary(monkeypatch, status):
    monkeypatch.setattr(function_app.checkpoint_store, "initialize", AsyncMock())
    summary = RunSummary(run_id="failed-run", status=status, started_at=datetime.now(UTC))
    monkeypatch.setattr(
        function_app,
        "build_ingestion_jobs",
        lambda *_: {
            "levelup": AsyncMock(return_value=summary),
        },
    )
    with pytest.raises(RuntimeError, match="levelup"):
        await function_app.run_configured_ingestions()
