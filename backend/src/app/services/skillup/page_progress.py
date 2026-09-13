"""Reject truncated or repeated live pagination before advancing a watermark."""

from dataclasses import dataclass


@dataclass
class PageProgress:
    total: int | None = None
    received: int = 0

    def observe(self, requested: int, page: int, total: int, count: int, has_next: bool) -> None:
        empty = page == total == count == 0 and not has_next
        if page != requested and not empty:
            raise ValueError("SkillUp returned an unexpected page")
        if self.total is None:
            self.total = total
        if total != self.total:
            raise ValueError("SkillUp total changed during pagination")
        self.received += count
        if has_next and (not count or self.received >= total):
            raise ValueError("SkillUp advertised an invalid next page")
        if not has_next and self.received != total:
            raise ValueError("SkillUp ended before the advertised total")
