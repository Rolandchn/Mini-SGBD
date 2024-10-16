
import Column

from Buffer import Buffer
from Record import Record


class Relation:
    def __init__(self, name: str, nb_column: int):
        self.name = name
        self.nb_column = nb_column
        
        self.columns: tuple[Column.ColumnInfo] = tuple()
    

    def writeRecordToBuffer(record: Record, buff: Buffer, pos: int) -> int:
        buff.byteBuffer.set_position(pos)

        # template
        for stuff in record.values:
            try:
                # check
                int(stuff)

                # cas où c'est un float
                if "." in stuff:
                    buff.byteBuffer.put_float(float(stuff))

                # cas où c'est un int
                else:
                    buff.byteBuffer.put_int(int(stuff))

            # cas où c'est un pur string
            except (ValueError):
                # doit écrire tout le string, caractère par caractère 
                buff.byteBuffer.put_char(stuff)
        # template end


    def readFromBuffer(record: Record, buff: Buffer, pos: int) -> int:
        ...