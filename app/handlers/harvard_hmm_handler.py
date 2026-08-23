from __future__ import annotations

from typing import Any

from app.config import Settings
from app.handlers.harvard_handler import run_harvard_ingestion
from app.models import RunSummary


async def run_harvard_hmm_ingestion(
    settings: Settings, **kwargs: Any
) -> RunSummary:
    return await run_harvard_ingestion(settings, "harvard_hmm", **kwargs)
