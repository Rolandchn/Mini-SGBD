from typing import List, Optional
from IRecordIterator import IRecordIterator
from Record import Record
from typing import List, Optional
from IRecordIterator import IRecordIterator
from Record import Record

class ProjectOperator(IRecordIterator):
    def __init__(self, iterator: IRecordIterator, columns: List[str], relation: 'Relation', table_alias: Optional[str] = None):
        self.iterator = iterator
        self.columns = columns
        self.relation = relation
        self.table_alias = table_alias
        self.column_indices = [self.get_column_index(col) for col in self.columns]

    def GetNextRecord(self) -> Optional[Record]:
        record = self.iterator.GetNextRecord()
        if record is None:
            return None
        projected_values = [record.values[idx] for idx in self.column_indices]
        return Record(projected_values)

    def Close(self):
        self.iterator.Close()

    def Reset(self):
        self.iterator.Reset()

    def get_column_index(self, column_name: str) -> int:
        # remp l'alias par le nom de col
        real_column_name = self.translate_column_alias(column_name)
        for i, col in enumerate(self.relation.columns):
            if col.name == real_column_name:
                return i
        raise ValueError(f"Column {column_name} not found in relation")

    def translate_column_alias(self, column_name: str) -> str:
        # Verifie si le nom de la colonne contient un alias
        if '.' in column_name:
            alias, real_name = column_name.split('.')
            if self.table_alias and alias == self.table_alias:
                return real_name
            else:
                raise ValueError(f"Table alias {alias} does not match the expected alias {self.table_alias}")
        return column_name
