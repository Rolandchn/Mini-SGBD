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

