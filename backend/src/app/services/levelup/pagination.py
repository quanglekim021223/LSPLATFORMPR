from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def is_last_page(
    payload: dict[str, Any], records_returned: int, offset: int, page_size: int
) -> bool:
    total_items = payload.get("totalItems")
    returned_items = payload.get("returnedItems")
    if isinstance(total_items, int) and isinstance(returned_items, int):
        return returned_items == 0 or offset + returned_items >= total_items
    return records_returned < page_size


def latest_timestamp(values: list[str]) -> str | None:
    if not values:
        return None
    return max(values, key=_timestamp_key)


def incremental_filter(watermark: str) -> str:
    return f"dateEdited gt '{watermark}'"


def _timestamp_key(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
