from dataclasses import dataclass

from ByteBuffer import ByteBuffer
from PageId import PageId


@dataclass
class Buffer(ByteBuffer):
    pageId: PageId = None

    dirty_flag: bool = False
    pin_count: int = 0
    
    def __init__(self, size=30):
        super().__init__(size)


if __name__ == "__main__":
    b = Buffer()
    b.pageId = PageId(1, 1)
    print(b)