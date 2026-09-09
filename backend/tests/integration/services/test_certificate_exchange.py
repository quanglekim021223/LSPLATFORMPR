"""Exercise the application service directly, without the local CLI wrapper."""

from pathlib import Path

import httpx
from pydantic import SecretStr

from app.clients.akajob_client import AkajobClient
from app.clients.fhu_client import FHUClient
from app.clients.skillup_certificate_client import SkillUpCertificateClient
from app.mocks.learning_exchange import create_demo_app
from app.mocks.learning_exchange_data import DEMO_API_KEY
from app.repositories.local_writer import LocalBronzeWriter
from app.services.skillup.certificate_exchange import run_certificate_exchange


async def test_service_accepts_independent_clients_mapping_and_storage(tmp_path: Path) -> None:
    app = create_demo_app(processing_delay=0)
    calls: list[tuple[str, str]] = []

    async def record(request: httpx.Request) -> None:
        calls.append((request.url.host, request.url.path))

    # All hosts route to ASGI in this test; no external network or real credentials.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        headers={"x-api-key": DEMO_API_KEY},
        event_hooks={"request": [record]},
    ) as client:
        result = await run_certificate_exchange(
            FHUClient(client, "https://fhu.test/fhu"),
            AkajobClient(client, "https://akajob.test/akajob"),
            SkillUpCertificateClient(
                client, "https://skillup.test/skillup", SecretStr(DEMO_API_KEY)
            ),
            LocalBronzeWriter(tmp_path),
            {
                "python": {"taxonomySkillId": 97915, "skillId": 93285, "skillName": "Python"},
                "sql": {"taxonomySkillId": 97916, "skillId": 93286, "skillName": "SQL"},
            },
        )
    assert result["status"] == "succeeded"
    assert result["bronze_pages"] == 5
    assert "mode" not in result
    assert calls[0] == ("fhu.test", "/fhu/employees")
    assert calls[1] == ("akajob.test", "/akajob/certificates")
    assert {host for host, _ in calls[2:]} == {"skillup.test"}
    assert all(path != "/demo/info" for _, path in calls)
