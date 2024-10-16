
import Column

from Buffer import Buffer
from Record import Record


class Relation:
    def __init__(self, name: str, num_column: int):
        self.name = name
        self.num_column = num_column
        
        self.columns: tuple[Column.ColumnType] = tuple()
    

    def writeRecordToBuffer(record: Record, buff: Buffer, pos: int) -> int:
        buff.byteBuffer.set_position(pos)
        for stuff in record.values:
            try:
                buff.byteBuffer.put_int(float(stuff))

                # cas où stuff est un int ?

            except (TypeError, ValueError):
                # doit écrire tout le string, caractère par caractère 
                buff.byteBuffer.put_char(stuff)