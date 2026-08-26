from __future__ import annotations

from typing import Any


def is_last_page(
    payload: dict[str, Any], records_returned: int, offset: int, page_size: int
) -> bool:
    total_items = payload.get("totalItems")
    returned_items = payload.get("returnedItems")
    if isinstance(total_items, int) and isinstance(returned_items, int):
        return returned_items == 0 or offset + returned_items >= total_items
    return records_returned < page_size
