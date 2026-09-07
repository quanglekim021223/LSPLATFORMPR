from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from functools import partial
from uuid import uuid4

import httpx
import pytest

import app.main as main
from app.auth.auth import create_access_token
from app.core.config import Settings
from app.models import RunStatus
from app.repositories import CheckpointStore
from app.services.fams.service import run_fams_ingestion

VENDORS = (
    "levelup", "skillup", "datacamp", "coursera", "linkedin",
    "harvard-hmm", "harvard-spark", "fams",
)


def auth_headers(settings: Settings, user: str = "test-admin") -> dict[str, str]:
    token = create_access_token(
        user_id=user,
        secret_key=settings.auth_jwt_secret.get_secret_value(),
        expire_minutes=10,
    )
    return {"Authorization": f"Bearer {token}"}


async def wait_for_completion(client: httpx.AsyncClient, url: str) -> dict:
    async with asyncio.timeout(5):
        while True:
            response = await client.get(url)
            assert response.status_code == 200
            result = response.json()
            if result["status"] != "running":
                return result
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("vendor", VENDORS)
async def test_pull_dispatches_only_selected_vendor_and_returns_live_run(
    settings_factory: Callable[..., Settings],
    monkeypatch: pytest.MonkeyPatch,
    vendor: str,
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)
    gate = asyncio.Event()
    called: list[str] = []

    def make_runner(key: str):
        async def runner(config, *, checkpoint_store, bronze_writer, on_started):
            assert config is settings
            assert checkpoint_store is store
            run_id = str(uuid4())
            await store.acquire_lock(key, run_id, 60)
            try:
                await store.start_run(run_id, key)
                called.append(key)
                on_started(run_id)
                await gate.wait()
                await store.finish_run(run_id, RunStatus.SUCCEEDED)
            finally:
                await store.release_lock(key, run_id)
        return runner

    for slug in VENDORS:
        key = slug.replace("-", "_")
        monkeypatch.setattr(main, f"run_{key}_ingestion", make_runner(key))

    application = main.create_app(settings, checkpoint_store=store)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application),
            base_url="http://test", headers=auth_headers(settings),
        ) as client:
            results = await asyncio.gather(
                client.post(f"/jobs/{vendor}/pull"),
                client.post(f"/jobs/{vendor}/pull"),
            )
            assert sorted(result.status_code for result in results) == [202, 409]
            accepted = next(result for result in results if result.status_code == 202)
            body = accepted.json()
            assert body["status"] == "accepted"
            assert body["vendor"] == vendor.replace("-", "_")
            assert called == [body["vendor"]]
            assert accepted.headers["Location"] == body["status_url"]
            running = await client.get(body["status_url"])
            assert running.json()["status"] == "running"
            assert running.json()["run_id"] == body["run_id"]

            # Different vendors do not block each other.
            other = "fams" if vendor != "fams" else "levelup"
            assert (await client.post(f"/jobs/{other}/pull")).status_code == 202
            gate.set()
            finished = await wait_for_completion(client, body["status_url"])
            assert finished["status"] == "succeeded"
            latest = await client.get(f"/jobs/{vendor}/latest")
            assert latest.json()["run_id"] == body["run_id"]


@pytest.mark.asyncio
async def test_pull_auth_configuration_and_schedule_lock(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(fams_api_key="")
    store = CheckpointStore(settings.checkpoint_db_path)
    application = main.create_app(settings, checkpoint_store=store)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            assert (await client.post("/jobs/levelup/pull")).status_code == 401
            client.headers.update(auth_headers(settings, "not-admin"))
            assert (await client.post("/jobs/levelup/pull")).status_code == 403
            client.headers.update(auth_headers(settings))
            assert (await client.post("/jobs/unknown/pull")).status_code == 404
            assert (await client.post("/jobs/fams/pull")).status_code == 503
            assert (await client.get(f"/jobs/runs/{uuid4()}")).status_code == 404
            assert (await client.get("/jobs/runs/not-a-uuid")).status_code == 422
            await store.acquire_lock("levelup", "schedule-run", 60)
            try:
                assert (await client.post("/jobs/levelup/pull")).status_code == 409
                assert await store.latest_run("levelup") is None
            finally:
                await store.release_lock("levelup", "schedule-run")


@pytest.mark.asyncio
async def test_pull_startup_error_is_sanitized(
    settings_factory: Callable[..., Settings], monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken_runner(*args, **kwargs):
        raise ValueError("upstream-secret-must-not-leak")

    monkeypatch.setattr(main, "run_fams_ingestion", broken_runner)
    settings = settings_factory()
    application = main.create_app(settings)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
            headers=auth_headers(settings),
        ) as client:
            response = await client.post("/jobs/fams/pull")
            assert response.status_code == 503
            assert "upstream-secret" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", [False, True])
async def test_real_fams_pull_writes_raw_or_cleans_up_on_shutdown(
    settings_factory: Callable[..., Settings], monkeypatch: pytest.MonkeyPatch,
    interrupt: bool,
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)
    gate = asyncio.Event()
    raw = json.dumps({
        "success": True, "message": "ok", "error_code": "",
        "data": {
            "classList": [{"id": 1, "site": "HN", "courseCode": "c1",
                           "courseName": "Course 1", "courseStatus": "CLOSED"}],
            "studentList": [],
        },
    }, indent=2).encode()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/fsa-reports/training-data"
        await gate.wait()
        return httpx.Response(200, content=raw, request=request)

    monkeypatch.setattr(main, "run_fams_ingestion", partial(
        run_fams_ingestion, transport=httpx.MockTransport(handler),
    ))
    application = main.create_app(settings, checkpoint_store=store)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
            headers=auth_headers(settings),
        ) as client:
            accepted = await client.post("/jobs/fams/pull")
            assert accepted.status_code == 202
            body = accepted.json()
            if not interrupt:
                gate.set()
                summary = await wait_for_completion(client, body["status_url"])
                assert summary["status"] == "succeeded"
                assert summary["records_by_domain"] == {"training_data": 1}
                raw_file = next(settings.bronze_local_path.rglob("offset=000001.json"))
                assert raw_file.read_bytes() == raw

    summary = await store.get_run(body["run_id"])
    assert summary is not None
    assert summary.status == (RunStatus.FAILED if interrupt else RunStatus.SUCCEEDED)
    # Neither a completed pull nor graceful cancellation leaves the vendor locked.
    await store.acquire_lock("fams", "next-run", 60)
    await store.release_lock("fams", "next-run")
