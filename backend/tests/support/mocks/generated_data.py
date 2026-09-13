from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

_BACKEND_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DATA_DIRECTORY = _BACKEND_ROOT / "data/mock/performance"
_VENDORS = (
    "levelup",
    "skillup",
    "datacamp",
    "coursera",
    "linkedin",
    "harvard_hmm",
    "harvard_spark",
    "fams",
)


def _automatic_data_directory() -> Path | None:
    if os.getenv("MOCK_DISABLE_PERFORMANCE_DATA", "").lower() in {"1", "true"}:
        return None
    if all((_DEFAULT_DATA_DIRECTORY / f"{vendor}.json").is_file() for vendor in _VENDORS):
        return _DEFAULT_DATA_DIRECTORY
    return None


@lru_cache
def generated_vendor_data(vendor: str) -> dict[str, Any] | None:
    """Load a generated vendor dataset when performance mock data is configured."""
    raw_directory = os.getenv("MOCK_DATA_DIR", "").strip()
    data_directory = Path(raw_directory) if raw_directory else _automatic_data_directory()
    if data_directory is not None:
        path = data_directory / f"{vendor}.json"
        if not path.is_file():
            raise RuntimeError(f"Generated mock data file does not exist: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise RuntimeError(f"Generated mock data is invalid for vendor: {vendor}")
        return data

    # Temporary compatibility with the previous single-file performance dataset.
    raw_path = os.getenv("MOCK_DATA_FILE", "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_file():
        raise RuntimeError(f"Generated mock data file does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    data = payload.get("vendors", {}).get(vendor)
    if not isinstance(data, dict):
        raise RuntimeError(f"Generated mock data is missing vendor: {vendor}")
    return data
