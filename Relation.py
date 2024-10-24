from typing import List
import Column

from Buffer import Buffer
from Record import Record


class Relation:
    def __init__(self, name: str, nb_column: int, columns: List[Column.ColumnInfo]):
        self.name = name
        
        self.nb_column = nb_column
        self.columns = columns
    

    def writeRecordToBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        if not self.has_varchar(self.columns):
            buff.set_position(pos)

            for index, stuff in enumerate(record.values):
                data = self.columns[index]

                if isinstance(data, Column.Number):
                    if type(data.value) == float:
                        buff.put_float(float(stuff))
                    
                    else:
                        buff.put_int(int(stuff))

                elif isinstance(data, Column.Char):
                    if isinstance(data, Column.CharVar):
                        pass
                    else:
                        buff.put_char(stuff)


        else:
            ...
        
    @staticmethod
    def has_varchar(columns):
        for stuff in columns:
            if isinstance(stuff, Column.CharVar):
                return True
            
        return False

    def readFromBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        buff.set_position(pos)

        for column in self.columns:
            ...