from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.clients.skillup_client import SkillUpResponseContractError
from app.fabric_state import BlobState
from app.schemas.skillup import validate_assessment_history
from app.services.coursera.course_catalog import ingest_catalog_pipeline as coursera_catalog
from app.services.linkedin.course_catalog import ingest_catalog_pipeline as linkedin_catalog
from app.services.skillup.page_progress import PageProgress
from tests.support.mocks.coursera import course_payload


@pytest.mark.parametrize("vendor", ["coursera", "linkedin"])
async def test_fabric_catalog_filters_without_optional_details(settings_factory, vendor):
    settings = settings_factory(fabric_enabled=True)
    payload = {
        "elements": [
            {
                "id": "Course~one" if vendor == "coursera" else "one",
                "contentId": "one" if vendor == "coursera" else None,
                "optional_missing": None,
            }
        ],
        "paging": {"total": 1, "links": []},
    }
    import json

    client = MagicMock()
    if vendor == "coursera":
        detail = {
            "elements": [course_payload("one", "Course One")],
            "paging": {},
            "linked": {},
        }
        client.get_json = AsyncMock(
            side_effect=[
                (payload, json.dumps(payload).encode()),
                (detail, json.dumps(detail).encode()),
            ]
        )
        client.content_detail_path.return_value = "/test-org/contents/one"
    else:
        client.get_json = AsyncMock(return_value=(payload, json.dumps(payload).encode()))
    store, writer = AsyncMock(), AsyncMock()
    store.courses_to_process.return_value = ["one"] if vendor == "coursera" else []
    pipeline = coursera_catalog if vendor == "coursera" else linkedin_catalog
    kwarg = "modified_since_timestamp" if vendor == "coursera" else "last_modified_after"
    result = await pipeline(
        settings,
        client,
        store,
        writer,
        "run",
        "2026-09-11",
        **{kwarg: 123, "sync_watermark": "456"},
    )
    assert all(item.succeeded for item in result)
    assert client.get_json.await_count == (2 if vendor == "coursera" else 1)
    params = client.get_json.await_args_list[0].args[1]
    field = (
        "modifiedSinceTimestamp"
        if vendor == "coursera"
        else "assetFilteringCriteria.lastModifiedAfter"
    )
    assert params[field] == 123
    store.set_watermark.assert_awaited_once_with(vendor, "course_catalog", "456", "run")
    assert writer.write_page.await_count == (2 if vendor == "coursera" else 1)
    if vendor == "coursera":
        store.add_courses.assert_awaited_once_with("run", ["one"])


def test_empty_skillup_page_zero_is_valid():
    payload = {
        "reports": [],
        "pageNumber": 0,
        "totalPages": 0,
        "totalCount": 0,
        "hasPreviousPage": False,
        "hasNextPage": False,
    }
    assert validate_assessment_history(payload).reports == []
    PageProgress().observe(1, 0, 0, 0, False)
    payload["totalCount"] = 50
    with pytest.raises(SkillUpResponseContractError):
        validate_assessment_history(payload)


def test_skillup_detects_silent_page_cap():
    progress = PageProgress()
    progress.observe(1, 1, 100, 50, True)
    with pytest.raises(ValueError, match="total changed"):
        progress.observe(2, 0, 0, 0, False)
    capped_progress = PageProgress()
    with pytest.raises(ValueError, match="before the advertised"):
        capped_progress.observe(1, 1, 100, 50, False)


def test_lost_blob_lease_prevents_state_commit(tmp_path):
    blob = MagicMock()
    path = tmp_path / "state.zip"
    path.write_bytes(b"test")
    with BlobState(blob) as state:
        blob.acquire_lease.assert_called_once_with(lease_duration=60)
        state.upload(path)
        state.lost.set()
        with pytest.raises(RuntimeError, match="lease was lost"):
            state.upload(path)
    assert blob.upload_blob.call_count == 2  # initial empty blob and one state upload


async def test_harvard_scans_available_old_files_without_requesting_missing_days(settings_factory):
    from app.models.harvard import RemoteFile, RemoteFileMetadata, vendor_config
    from app.repositories import CheckpointStore
    from app.services.harvard.learning_history import ingest_learning_history
    from tests.support.mocks.harvard import history_csv

    settings = settings_factory(fabric_enabled=True, harvard_hmm_history_start_date="2000-01-01")
    vendor = vendor_config(settings, "harvard_hmm")
    store = CheckpointStore(settings.checkpoint_db_path)
    await store.initialize()
    await store.start_run("run", "harvard_hmm")
    name = f"{vendor.report_filename_prefix}20260901.csv"
    path = f"{settings.harvard_sftp_remote_dir}/{name}"
    raw = history_csv("harvard_hmm")
    timestamp = datetime(2026, 9, 2, tzinfo=UTC)
    transport, writer = AsyncMock(), AsyncMock()
    transport.list_files.return_value = [
        RemoteFileMetadata(
            remote_path=path,
            file_name=name,
            size=len(raw),
            modified_at=timestamp,
        )
    ]
    transport.fetch.return_value = RemoteFile(
        remote_path=path,
        file_name=name,
        content=raw,
        size=len(raw),
        modified_at=timestamp,
    )
    for run_id in ["run", "second"]:
        if run_id == "second":
            await store.start_run(run_id, "harvard_hmm")
        await ingest_learning_history(
            settings,
            vendor,
            transport,
            store,
            writer,
            run_id,
            "2026-09-11",
            now=lambda: datetime(2026, 9, 11, tzinfo=UTC),
        )
    transport.fetch.assert_awaited_once_with(path)
    writer.write_file.assert_awaited_once()


async def test_assessment_splits_truncated_window_before_writing(settings_factory):
    import json
    from datetime import timedelta

    from app.services.skillup.assessment_history import ingest_assessment_history
    from app.services.skillup.assessment_windows import parse_time

    settings = settings_factory(fabric_enabled=True)
    client, store, writer = MagicMock(), AsyncMock(), AsyncMock()
    seen = []

    async def fetch(base, path, params):
        lower, upper = parse_time(params["startDate"]), parse_time(params["endDate"])
        truncated = upper - lower > timedelta(minutes=30)
        seen.append((lower, upper))
        payload = {
            "reports": [{"testInvitationId": lower.isoformat()}],
            "pageNumber": 1,
            "totalCount": 2 if truncated else 1,
            "hasNextPage": False,
        }
        return payload, json.dumps(payload).encode()

    client.get_json = AsyncMock(side_effect=fetch)
    await ingest_assessment_history(
        settings,
        client,
        store,
        writer,
        "run",
        "2026-09-11",
        start_date="2026-09-10T00:00:00Z",
        end_date="2026-09-10T01:00:00Z",
        daily_sync_watermark="2026-09-10T01:00:00Z",
    )
    assert len(seen) == 3
    assert writer.write_page.await_count == 2
    assert [call.args[0].offset for call in writer.write_page.call_args_list] == [1, 2]
    assert seen[1][1] + timedelta(microseconds=1) == seen[2][0]
    store.set_watermark.assert_awaited_once()


async def test_unstable_assessment_never_advances_watermark(settings_factory):
    from app.services.skillup.assessment_history import ingest_assessment_history

    settings = settings_factory(fabric_enabled=True)
    client, store, writer = MagicMock(), AsyncMock(), AsyncMock()
    payload = {"reports": [], "pageNumber": 0, "totalCount": 10, "hasNextPage": False}
    client.get_json = AsyncMock(return_value=(payload, b'{"reports":[]}'))
    with pytest.raises(ValueError):
        await ingest_assessment_history(
            settings,
            client,
            store,
            writer,
            "run",
            "2026-09-11",
            start_date="2026-09-10T00:00:00Z",
            end_date="2026-09-10T00:10:00Z",
            daily_sync_watermark="2026-09-10T00:10:00Z",
        )
    writer.write_page.assert_not_awaited()
    store.set_watermark.assert_not_awaited()
