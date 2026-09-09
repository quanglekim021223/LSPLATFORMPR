from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.clients.certificate_exchange_http import CertificateExchangeError
from app.commands.learning_exchange import run_local_exchange, validate_demo_url
from app.mocks.learning_exchange import create_demo_app
from app.mocks.learning_exchange_data import DEMO_API_KEY, SOURCE_FILE

BASE_URL = "http://127.0.0.1:9100"
HEADERS = {"x-api-key": DEMO_API_KEY}
CREATE_PATH = "/skillup/certificates/create"
UPLOAD_PATH = "/skillup/employees/DEMO-EMP-001/certificates"


@pytest.fixture
def source_file(tmp_path: Path) -> Path:
    path = tmp_path / "sources.json"
    path.write_bytes(SOURCE_FILE.read_bytes())
    return path


def client_for(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL)


async def test_end_to_end_uses_http_and_persists_raw_results(
    source_file: Path,
    tmp_path: Path,
) -> None:
    app = create_demo_app(source_file, processing_delay=0.04)
    calls: list[tuple[str, str]] = []

    async def record(request: httpx.Request) -> None:
        calls.append((request.method, request.url.path))

    async with client_for(app) as client:
        client.event_hooks["request"] = [record]
        result = await run_local_exchange(client, BASE_URL, tmp_path / "bronze", poll_interval=0.01)

    assert result["status"] == "succeeded"
    assert result["polls"] > 1
    assert calls[:5] == [
        ("GET", "/demo/info"),
        ("GET", "/fhu/employees"),
        ("GET", "/akajob/certificates"),
        ("POST", CREATE_PATH),
        ("POST", UPLOAD_PATH),
    ]
    assert len(app.state.demo_catalog) == len(app.state.demo_uploads) == 1
    root = tmp_path / "bronze"
    assert list((root / "fhu/employees").rglob("offset=*.json"))
    assert list((root / "akajob/certificates").rglob("offset=*.json"))
    profiles = sorted((root / "skillup/skill_inventory").rglob("offset=*.json"))
    first = json.loads(profiles[0].read_bytes())["items"][0]
    last = json.loads(profiles[-1].read_bytes())["items"][0]
    assert first["skills"] == []
    assert {item["skill"]["skillName"] for item in last["skills"]} == {"Python", "SQL"}
    assert all(DEMO_API_KEY not in path.read_text() for path in root.rglob("*.json"))


async def test_rerun_mock_is_idempotent_but_stores_separate_raw_runs(
    source_file: Path,
    tmp_path: Path,
) -> None:
    app = create_demo_app(source_file, processing_delay=0)
    async with client_for(app) as client:
        first = await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
        second = await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
    assert first["run_id"] != second["run_id"]
    assert len(app.state.demo_catalog) == len(app.state.demo_uploads) == 1


async def test_source_changes_are_visible_without_restarting_mock(
    source_file: Path,
    tmp_path: Path,
) -> None:
    app = create_demo_app(source_file, processing_delay=0)
    async with client_for(app) as client:
        await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
        data = json.loads(await asyncio.to_thread(source_file.read_bytes))
        data["certificates"][0]["licenseNumber"] = "DEMO-EDITED"
        data["certificates"].append(
            {
                **data["certificates"][0],
                "awardId": "DEMO-AWARD-002",
                "title": "Demo SQL",
                "skillCodes": ["sql"],
            }
        )
        await asyncio.to_thread(source_file.write_text, json.dumps(data))
        result = await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
    assert result["awards"] == 2
    assert len(app.state.demo_uploads) == 2
    assert all(item[0].license_number == "DEMO-EDITED" for item in app.state.demo_uploads.values())


@pytest.mark.parametrize(
    "change, message",
    [
        ({"skillCodes": ["unknown"]}, "taxonomy mapping"),
        ({"externalEmployeeId": "UNKNOWN"}, "missing from FHU"),
        ({"isStandardCertificate": False}, "standard certificate"),
    ],
)
async def test_preflight_stops_invalid_mapping_before_post(
    source_file: Path,
    tmp_path: Path,
    change: dict[str, Any],
    message: str,
) -> None:
    data = json.loads(await asyncio.to_thread(source_file.read_bytes))
    data["certificates"][0].update(change)
    await asyncio.to_thread(source_file.write_text, json.dumps(data))
    app = create_demo_app(source_file)
    async with client_for(app) as client:
        with pytest.raises(CertificateExchangeError, match=message):
            await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
    assert app.state.demo_catalog == app.state.demo_uploads == {}


async def test_employee_missing_from_skillup_is_not_auto_enrolled(
    source_file: Path,
    tmp_path: Path,
) -> None:
    app = create_demo_app(source_file)
    data = json.loads(await asyncio.to_thread(source_file.read_bytes))
    data["employees"][0]["externalEmployeeId"] = "NEW-EMPLOYEE"
    data["certificates"][0]["externalEmployeeId"] = "NEW-EMPLOYEE"
    await asyncio.to_thread(source_file.write_text, json.dumps(data))
    async with client_for(app) as client:
        with pytest.raises(CertificateExchangeError, match="HTTP 404"):
            await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
    assert app.state.demo_uploads == {}


async def test_accepted_upload_is_not_treated_as_processing_success(
    source_file: Path,
    tmp_path: Path,
) -> None:
    app = create_demo_app(source_file, processing_delay=3600)
    async with client_for(app) as client:
        with pytest.raises(CertificateExchangeError, match="not observed"):
            await run_local_exchange(
                client, BASE_URL, tmp_path / "bronze", max_polls=2, poll_interval=0
            )
    assert len(app.state.demo_uploads) == 1
    assert len(list((tmp_path / "bronze/skillup/skill_inventory").rglob("offset=*.json"))) == 2


async def test_failed_post_is_not_retried(source_file: Path, tmp_path: Path) -> None:
    app = create_demo_app(source_file)
    attempts = 0

    @app.middleware("http")
    async def fail_create(request: Request, call_next: Any) -> Any:
        nonlocal attempts
        if request.url.path == CREATE_PATH:
            attempts += 1
            return JSONResponse({"detail": "simulated secret-containing failure"}, status_code=500)
        return await call_next(request)

    async with client_for(app) as client:
        with pytest.raises(CertificateExchangeError, match="POST failed: HTTP 500") as error:
            await run_local_exchange(client, BASE_URL, tmp_path / "bronze")
    assert attempts == 1
    assert "secret-containing" not in str(error.value)


async def test_documented_write_contracts_and_mock_auth(source_file: Path) -> None:
    app = create_demo_app(source_file, processing_delay=0)
    certificate = {
        "title": "Demo",
        "certificateIssuer": "Demo issuer",
        "skills": [{"taxonomySkillId": 97915}],
    }
    upload = {"title": "Demo", "issuer": "Demo issuer", "isStandardCertificate": True}
    async with client_for(app) as client:
        assert (await client.post(CREATE_PATH, json=certificate)).status_code == 401
        response = await client.post(CREATE_PATH, json=certificate, headers=HEADERS)
        assert response.status_code == 200
        assert response.json() == certificate
        assert (await client.post(UPLOAD_PATH, json=upload, headers=HEADERS)).status_code == 422
        response = await client.post(UPLOAD_PATH, json=[upload], headers=HEADERS)
        assert response.status_code == 201 and response.json() == {}
        upload.pop("isStandardCertificate")
        assert (await client.post(UPLOAD_PATH, json=[upload], headers=HEADERS)).status_code == 422
        certificate["skills"] = [{"taxonomySkillId": "97915"}]
        response = await client.post(CREATE_PATH, json=certificate, headers=HEADERS)
        assert response.status_code == 422


@pytest.mark.parametrize(
    "url",
    [
        "https://api.skillsintelligence.imocha.io",
        "http://localhost:9100",
        "http://127.0.0.1:9100/skillup",
        "http://user:secret@127.0.0.1:9100",
    ],
)
def test_rejects_non_demo_origins(url: str) -> None:
    with pytest.raises(CertificateExchangeError, match="loopback"):
        validate_demo_url(url)


async def test_rejects_server_without_demo_marker(tmp_path: Path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={"service": "not-our-mock"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(CertificateExchangeError, match="dedicated"):
            await run_local_exchange(client, BASE_URL, tmp_path)
    assert calls == ["/demo/info"]
