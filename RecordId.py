from dataclasses import dataclass

from PageId import PageId

@dataclass(frozen=True, order=True)
class RecordId:
    pageId: PageId
    slotIdx: int


if __name__ == "__main__":
    r = RecordId(PageId(1, 1), 2)
    print(r)