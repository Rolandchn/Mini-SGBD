from dataclasses import dataclass

from PageId import PageId

@dataclass(order=True)
@dataclass(order=True)
class RecordId:
    pageId: PageId
    slotIdx: int = 0

    def setSlotIdx(self, slotIdx):
        self.slotIdx = slotIdx
