from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from app.models import RunStatus
from app.models.harvard import HarvardVendorConfig
from app.services.coursera.service import CourseraJob
from app.services.datacamp.service import DataCampJob
from app.services.fams.service import FAMSJob
from app.services.harvard.service import HarvardJob
from app.services.linkedin.service import LinkedInJob
from app.services.skillup.service import SkillUpJob


def _standard_job(job_type: Callable[..., Any], settings: object) -> Any:
    client = Mock()
    client.sensitive_values.return_value = ()
    return job_type(settings, client, AsyncMock(), Mock())


def _harvard_job(settings: object) -> HarvardJob:
    vendor = HarvardVendorConfig(
        vendor="harvard_hmm",
        display_name="Harvard HMM",
        catalog_code="HMM",
        client_id="",
        client_secret="",
        org_key="test-org",
        report_filename_prefix="harvard_hmm_reporting_",
    )
    client = Mock()
    client.sensitive_values.return_value = ()
    return HarvardJob(
        settings,  # type: ignore[arg-type]
        vendor,
        client,
        AsyncMock(),
        Mock(),
        sftp_transport=None,
        now=Mock(),
        sleep=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_factory",
    [
        lambda settings: _standard_job(CourseraJob, settings),
        lambda settings: _standard_job(DataCampJob, settings),
        lambda settings: _standard_job(FAMSJob, settings),
        _harvard_job,
        lambda settings: _standard_job(LinkedInJob, settings),
        lambda settings: _standard_job(SkillUpJob, settings),
    ],
    ids=["coursera", "datacamp", "fams", "harvard", "linkedin", "skillup"],
)
async def test_heartbeat_cancellation_is_recorded_then_reraised(
    job_factory: Callable[[object], object],
) -> None:
    settings = SimpleNamespace(
        coursera_lock_ttl_seconds=3600,
        fams_lock_ttl_seconds=3600,
        linkedin_lock_ttl_seconds=3600,
        harvard_secrets=lambda _vendor: (),
    )
    job = job_factory(settings)
    checkpoints = job.checkpoints  # type: ignore[attr-defined]

    async def cancel_after_start(*_args: object) -> None:
        job._heartbeat_error = RuntimeError("heartbeat stopped")  # type: ignore[attr-defined]
        raise asyncio.CancelledError

    checkpoints.start_run.side_effect = cancel_after_start
    with patch.object(job, "_heartbeat_loop", AsyncMock(return_value=None)):
        with pytest.raises(asyncio.CancelledError):
            await job.run()  # type: ignore[attr-defined]

    assert checkpoints.finish_run.await_count == 1
    finish_args = checkpoints.finish_run.await_args.args
    assert finish_args[1:] == (RunStatus.FAILED, "heartbeat stopped")
    checkpoints.release_lock.assert_awaited_once()
