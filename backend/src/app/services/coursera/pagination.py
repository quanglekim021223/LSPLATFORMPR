from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlsplit


def next_start(payload: dict[str, Any], current_start: int) -> int | None:
    paging = payload.get("paging")
    if not isinstance(paging, dict):
        return None
    value = paging.get("next")
    if value is None or value == "" or value is False:
        return None
    next_value: int
    if isinstance(value, int) and not isinstance(value, bool):
        next_value = value
    elif isinstance(value, str):
        if value.isdigit():
            next_value = int(value)
        else:
            starts = parse_qs(urlsplit(value).query).get("start", [])
            if len(starts) != 1 or not starts[0].isdigit():
                raise ValueError("Coursera paging.next must identify the next start")
            next_value = int(starts[0])
    else:
        raise ValueError("Coursera paging.next must identify the next start")
    if next_value <= current_start:
        raise ValueError("Coursera paging.next must advance start")
    return next_value
