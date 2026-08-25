from __future__ import annotations

from typing import Any

from app.core.config import Settings
from app.models import RunSummary
from app.services.harvard.service import run_harvard_ingestion


async def run_harvard_spark_ingestion(
    settings: Settings, **kwargs: Any
) -> RunSummary:
    return await run_harvard_ingestion(settings, "harvard_spark", **kwargs)
