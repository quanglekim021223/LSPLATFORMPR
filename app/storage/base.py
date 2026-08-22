from __future__ import annotations

from pathlib import Path
from typing import Protocol

from app.models import PageWrite


class BronzeWriter(Protocol):
    async def write_page(self, page: PageWrite) -> Path: ...
