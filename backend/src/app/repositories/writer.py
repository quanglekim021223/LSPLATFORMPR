from __future__ import annotations

from pathlib import Path
from typing import Protocol

from app.models import BinaryFileWrite, PageWrite


class BronzeWriter(Protocol):
    async def write_page(self, page: PageWrite) -> Path: ...

    async def write_file(self, file: BinaryFileWrite) -> Path: ...
