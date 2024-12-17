from typing import List, Optional
from IRecordIterator import IRecordIterator
from Condition import Condition
from Record import Record
from Relation import Relation
class SelectOperator(IRecordIterator):

    def __init__(self, iterator: IRecordIterator, conditions: List[Condition], relation: 'Relation'):
        self.iterator = iterator
        self.conditions = conditions
        self.relation = relation

    def GetNextRecord(self) -> Optional[Record]:
        while True:
            record = self.iterator.GetNextRecord()
            if record is None:
                return None

            condition_results = [condition.evaluate(record, self.relation.columns) for condition in self.conditions]
            if all(condition_results):
                return record
    def Close(self):
        self.iterator.Close()

    def Reset(self):
        self.iterator.Reset()
