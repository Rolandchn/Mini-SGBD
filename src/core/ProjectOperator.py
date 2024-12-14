from typing import List, Optional
from IRecordIterator import IRecordIterator
from Record import Record

class ProjectOperator(IRecordIterator):
    def __init__(self, iterator: IRecordIterator, columns: List[str], relation: 'Relation'):
        self.iterator = iterator
        self.columns = columns
        self.relation = relation

    def GetNextRecord(self) -> Optional[Record]:
        record = self.iterator.GetNextRecord()
        if record is None:
            return None
        projected_values = [record.values[self.get_column_index(col)] for col in self.columns]
        return Record(projected_values)

    def Close(self):
        self.iterator.Close()

    def Reset(self):
        self.iterator.Reset()

    def get_column_index(self, column_name: str) -> int:
        for i, col in enumerate(self.relation.columns):
            if col.name == column_name:
                return i
        raise ValueError(f"Column {column_name} not found in relation")
