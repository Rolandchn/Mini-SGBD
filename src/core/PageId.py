from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class PageId:
    fileIdx: int
    pageIdx: int


