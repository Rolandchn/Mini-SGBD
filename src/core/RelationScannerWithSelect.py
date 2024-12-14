from typing import List, Optional
from IRecordIterator import IRecordIterator
from Record import Record
from Relation import Relation
from Condition import Condition


class RelationScannerWithSelect(IRecordIterator):
    def __init__(self, relation: 'Relation', conditions: List[Condition]):
        self.relation = relation
        self.buffer_manager = relation.bufferManager
        self.conditions = conditions
        self.current_page_id = None
        self.current_buffer = None
        self.current_record_index = 0
        self.data_pages = relation.getDataPages()
        self.current_data_page_index = 0

    def GetNextRecord(self) -> Optional[Record]:
        while True:
            if self.current_buffer is None or self.current_record_index >= self.current_buffer.get_record_count():
                if self.current_data_page_index >= len(self.data_pages):
                    return None
                self.load_next_page()

            record = self.read_record_from_buffer()
            if record is not None and all(condition.evaluate(record, self.relation.columns) for condition in self.conditions):
                return record

    def Close(self):
        if self.current_buffer is not None:
            self.buffer_manager.FreePage(self.current_page_id)
            self.current_buffer = None

    def Reset(self):
        self.current_data_page_index = 0
        self.current_record_index = 0
        self.current_buffer = None
        self.current_page_id = None

    def load_next_page(self):
        if self.current_buffer is not None:
            self.buffer_manager.FreePage(self.current_page_id)

        self.current_page_id = self.data_pages[self.current_data_page_index]
        self.current_buffer = self.buffer_manager.getPage(self.current_page_id)
        self.current_record_index = 0
        self.current_data_page_index += 1

    def read_record_from_buffer(self) -> Optional[Record]:
        record = Record()
        pos = self.current_buffer.get_record_position(self.current_record_index)
        if pos == -1:
            return None
        self.relation.readFromBuffer(record, self.current_buffer, pos)
        self.current_record_index += 1
        return record
