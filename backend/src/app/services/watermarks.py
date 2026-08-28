from __future__ import annotations

from collections.abc import Iterable

from app.repositories import CheckpointStore


async def store_optional_watermarks(
    checkpoints: CheckpointStore,
    vendor: str,
    domain: str,
    run_id: str,
    watermarks: Iterable[tuple[str, str | None]],
) -> None:
    for scope, value in watermarks:
        if value is not None:
            await checkpoints.set_watermark(vendor, domain, value, run_id, scope)
