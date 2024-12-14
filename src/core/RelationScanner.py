from typing import Optional
from IRecordIterator import IRecordIterator
from Record import Record
from Relation import Relation

class RelationScanner(IRecordIterator):
    def __init__(self, relation: 'Relation'):
        self.relation = relation
        self.records = relation.GetAllRecords()
        self.current_index = 0

    def GetNextRecord(self) -> Optional[Record]:
        if self.current_index < len(self.records):
            record = self.records[self.current_index]
            self.current_index += 1
            return record
        return None

    def Close(self):
        self.records = []
        self.current_index = 0

    def Reset(self):
        self.current_index = 0
