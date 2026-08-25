from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlsplit


def next_start(payload: dict[str, Any], current_start: int) -> int | None:
    paging = payload.get("paging")
    if not isinstance(paging, dict):
        raise ValueError("LinkedIn response must contain paging object")
    links = paging.get("links")
    if not isinstance(links, list):
        raise ValueError("LinkedIn paging.links must be a list")
    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if not isinstance(href, str):
                raise ValueError("LinkedIn next paging link must contain href")
            starts = parse_qs(urlsplit(href).query).get("start", [])
            if len(starts) != 1 or not starts[0].isdigit():
                raise ValueError("LinkedIn next paging link must contain start")
            value = int(starts[0])
            if value <= current_start:
                raise ValueError("LinkedIn next paging start must advance")
            return value
    return None
