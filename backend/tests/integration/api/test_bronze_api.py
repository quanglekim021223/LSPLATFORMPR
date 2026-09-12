from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from app.auth.auth import create_access_token
from app.main import create_app
from app.models import PageWrite
from app.repositories import CheckpointStore, LocalBronzeWriter


def admin_headers(settings: object) -> dict[str, str]:
    token = create_access_token(
        user_id=settings.auth_admin_username,  # type: ignore[attr-defined]
        secret_key=settings.auth_jwt_secret.get_secret_value(),  # type: ignore[attr-defined]
        expire_minutes=10,
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_exports_persisted_bronze_and_confirmed_cleanup(
    settings_factory: Callable[..., object],
) -> None:
    settings = settings_factory()
    store = CheckpointStore(settings.checkpoint_db_path)  # type: ignore[attr-defined]
    writer = LocalBronzeWriter(settings.bronze_local_path)  # type: ignore[attr-defined]
    app = create_app(settings, checkpoint_store=store, bronze_writer=writer)  # type: ignore[arg-type]
    page = PageWrite(
        vendor="levelup",
        data_domain="course_catalog",
        ingestion_date="2026-09-07",
        run_id="levelup-run",
        offset=0,
        raw_payload=b'{"items":[{"id":"course-1"}]}',
        records_count=1,
        request_parameters={},
        fetched_at=datetime.now(UTC),
    )

    async with app.router.lifespan_context(app):
        await writer.write_page(page)
        await store.start_run(page.run_id, page.vendor)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers=admin_headers(settings),
        ) as client:
            exported = await client.get(
                "/bronze/export.csv",
                params={"vendor": "levelup"},
                headers={"Origin": "http://localhost:5173"},
            )
            assert exported.status_code == 200
            date = datetime.now(UTC).date().isoformat()
            assert exported.headers["access-control-expose-headers"] == ("Content-Disposition")
            assert exported.headers["content-disposition"] == (
                f'attachment; filename="bronze-levelup-{date}.csv"'
            )
            assert "course-1" in exported.text
            assert "levelup-run" in exported.text

            all_vendors = await client.get(
                "/bronze/export.csv",
                params=[("vendor", "levelup"), ("vendor", "skillup")],
            )
            assert all_vendors.headers["content-disposition"] == (
                f'attachment; filename="bronze-export-{date}.csv"'
            )

            await store.acquire_lock("levelup", page.run_id, 3600)
            refused = await client.request("DELETE", "/bronze", json={"vendors": ["levelup"]})
            assert refused.status_code == 409
            assert any(settings.bronze_local_path.rglob("offset=*.json"))  # type: ignore[attr-defined]
            await store.release_lock("levelup", page.run_id)

            cleared = await client.request("DELETE", "/bronze", json={"vendors": ["levelup"]})
            assert cleared.status_code == 200
            assert cleared.json()["runs_deleted"] == 1
            assert not (settings.bronze_local_path / "levelup").exists()  # type: ignore[attr-defined]
            assert await store.latest_run("levelup") is None
