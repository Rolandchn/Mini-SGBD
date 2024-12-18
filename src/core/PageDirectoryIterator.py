from typing import Optional
from PageId import PageId
from Relation import Relation
from Buffer import Buffer
from BufferManager import BufferManager
class PageDirectoryIterator:
    def __init__(self, relation:Relation):
        self.relation = relation
        self.current_page_id = self.relation.headerPageId
        buff = self.relation.bufferManager.getPage(self.relation.headerPageId)
        self.next_page_id = self.get_next_page_id(self.current_page_id,buff)
        self.relation.bufferManager.FreePage(self.relation.headerPageId)

    def get_next_page_id(self, current_page_id: PageId, buffer:Buffer) -> Optional[PageId]:
        if current_page_id == self.relation.headerPageId:
            buffer.set_position(0)
            nb = buffer.read_int()
            if nb == 0:
                return None
            fidx = buffer.read_int()
            pidx = buffer.read_int()
            buffer.set_position(buffer.getPos() + 4)
            return PageId(fidx, pidx)
        else:
            buffer.set_position(0)
            nb_pages = buffer.read_int()
            for i in range(nb_pages):
                fidx = buffer.read_int()
                pidx = buffer.read_int()
                buffer.set_position(buffer.getPos() + 4)
                if pidx == current_page_id.pageIdx and fidx == current_page_id.fileIdx:
                    if i == nb_pages - 1:
                        return None
                    else:
                        fidx = buffer.read_int()
                        pidx = buffer.read_int()
                        buffer.set_position(buffer.getPos() + 4)

                        return PageId(fidx, pidx)
            
    def GetNextDataPageId(self) -> Optional[PageId]:
        buff = self.relation.bufferManager.getPage(self.relation.headerPageId)
        if self.next_page_id is None:
            self.Close()
            return None
        current_page_id = self.next_page_id
        self.next_page_id = self.get_next_page_id(current_page_id, buff)
        self.Close()
        return current_page_id

    def Reset(self):
        self.current_page_id = self.relation.headerPageId
        self.next_page_id = self.get_next_page_id(self.current_page_id)

    def Close(self):
        self.relation.bufferManager.FreePage(self.relation.headerPageId)
        
