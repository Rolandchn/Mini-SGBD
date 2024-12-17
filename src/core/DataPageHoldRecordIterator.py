from IRecordIterator import IRecordIterator
from BufferManager import BufferManager
from PageId import PageId
from Relation import Relation
from Record import Record
class DataPageHoldRecordIterator(IRecordIterator):
    def __init__(self, buffer_manager: BufferManager, page_id: PageId, relation: Relation):
        self.buffer_manager = buffer_manager
        self.page_id = page_id
        self.relation = relation
        
        # Get the buffer for this page
        self.buffer = self.buffer_manager.getPage(page_id)
        
        # Initialize position and record count
        self.buffer.set_position(0)
        self.record_count = self.buffer.read_int()
        self.current_record_index = 0
    
    def GetNextRecord(self):
        if self.current_record_index >= self.record_count:
            return None
        
        # Calculate the position of the current record
        current_pos = 4  # Skip the record count
        for i in range(self.current_record_index):
            # Get size of previous records to find correct position
            prev_record = self._read_record_at_position(current_pos)
            current_pos += self.relation.getRecordSize(prev_record)
        
        # Read the current record
        record = self._read_record_at_position(current_pos)
        
        self.current_record_index += 1
        return record
    
    def _read_record_at_position(self, position):
        record  = Record([])
        self.relation.readFromBuffer(record, self.buffer,position)
        
        return record
    
    def Close(self):
        self.buffer_manager.FreePage(self.page_id)
        
    def Reset(self):
        return None