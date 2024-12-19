from IRecordIterator import IRecordIterator
from BufferManager import BufferManager
from PageId import PageId
from Relation import Relation
from Record import Record
class DataPageHoldRecordIterator(IRecordIterator):
    def __init__(self, page_id: PageId, relation: Relation):
        self.buffer_manager = relation.bufferManager
        self.page_id = page_id
        self.relation = relation
        
        # Get the buffer for this page
        self.buffer = self.buffer_manager.getPage(page_id)
        
        # Initialize position and record count
        self.buffer.set_position(self.buffer_manager.config.pagesize - 8)
        self.record_count = self.buffer.read_int()
        self.current_record_index = 0
    
    def GetNextRecord(self):
        if self.current_record_index >= self.record_count:
            return None

        self.current_record_index += 1
        pos = self.buffer_manager.config.pagesize - 8 - 8 * self.current_record_index
        self.buffer.set_position(pos)
        recordpos = self.buffer.read_int()
        return None if recordpos == -1 else self._read_record_at_position(recordpos)
        
    
    def _read_record_at_position(self, position):
        record  = Record([])
        self.relation.readFromBuffer(record, self.buffer,position)
        
        return record
    
    def Close(self):
        self.buffer_manager.FreePage(self.page_id)
        
    def Reset(self):
        self.current_record_index = 0
    

