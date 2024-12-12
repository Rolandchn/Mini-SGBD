from dataclasses import dataclass

from PageId import PageId

@dataclass(frozen=True, order=True)
class RecordId:
    pageId: PageId
    slotIdx: int

