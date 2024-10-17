from typing import List
import Column

from Buffer import Buffer
from Record import Record


class Relation:
    def __init__(self, name: str, nb_column: int):
        self.name = name
        self.nb_column = nb_column
        
        # est ce que ça doit forcément être un tuple ?
        self.columns: List[Column.ColumnInfo] = []
    

    def writeRecordToBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        buff.byteBuffer.set_position(pos)

        for stuff in record.values:
            try:
                int(stuff)

                # cas où c'est un float
                if "." in stuff:
                    buff.byteBuffer.put_float(float(stuff))
                    self.columns += Column.ColumnType.REAL

                # cas où c'est un int
                else:
                    buff.byteBuffer.put_int(int(stuff))
                    self.columns += Column.ColumnType.INT

            # cas où c'est un pur string
            except (ValueError):
                # doit écrire tout le string, caractère par caractère 
                buff.byteBuffer.put_char(stuff)
                self.columns += Column.ColumnType.CHAR
                
                # cas où c'est un pur string variable ?


    def readFromBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        buff.byteBuffer.set_position(pos)

        for column in self.columns:
            ...