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
        buff.set_position(pos)
        
        if not self.has_varchar(self.columns):
            # record = single row of values 
            for value, value_info in zip(record.values, self.columns):
                if isinstance(value_info.type, Column.Number):
                    if type(value) == float:
                        buff.put_float(float(value))
                    
                    else:
                        buff.put_int(int(value))
                
                elif isinstance(value_info.type, Column.Char):
                    for i in range(len(value)):
                        buff.put_char(value[i])

        else:
            init_pos = buff.__pos
            pos_buffer = buff.__pos + 4 * (self.nb_column + 1)
            
            buff.put_int(pos_buffer)
            init_pos = buff.__pos

            for index, value in enumerate(record.values):
                buff.set_position(pos_buffer)

                value_info = self.columns[index]

                if isinstance(value_info, Column.Number):
                    if type(value_info.value) == float:
                        buff.put_float(float(value))
                    
                    else:
                        buff.put_int(int(value))

                    buff.set_position(init_pos)
                    buff.put_int(pos_buffer + 4)

                    pos_buffer = buff.__pos + 4

                
                elif isinstance(value_info, Column.Char):
                    for i in range(value_info.size):
                        buff.put_char(value[i])

                    buff.set_position(init_pos)
                    buff.put_int(pos_buffer + value_info.size)

                    pos_buffer = buff.__pos
                
                else:
                    for i in range(value_info.size):
                        buff.put_char(value[i])

                    buff.set_position(init_pos)
                    buff.put_int(pos_buffer + value_info.size)

                    pos_buffer = buff.__pos


    def readFromBuffer(self, record, buff, pos) -> int:
        buff.set_position(pos)
        
        if not self.has_varchar(self.columns):
            for index, value in enumerate(record.values):
                data = self.columns[index]

                if isinstance(data, Column.Number):
                    if type(data.value) == float:
                        buff.read_float(float(value))
                    
                    else:
                        buff.read_int(int(value))
                
                elif isinstance(data, Column.Char):
                    for i in range(data.size):
                        buff.read_char(value[i])

        else:
            pos_buffer = buff.read_int()
            
            buff.put_int(pos_buffer)
            init_pos = buff.__pos

            for index, value in enumerate(record.values):
                buff.set_position(pos_buffer)

                data = self.columns[index]

                if isinstance(data, Column.Number):
                    if type(data.value) == float:
                        buff.read_float(float(value))
                    
                    else:
                        buff.read_int(int(value))

                    buff.set_position(init_pos)
                    buff.read_int(pos_buffer + 4)

                    pos_buffer = buff.__pos + 4

                
                elif isinstance(data, Column.Char):
                    for i in range(data.size):
                        buff.put_char(value[i])

                    buff.set_position(init_pos)
                    buff.read_int(pos_buffer + data.size)

                    pos_buffer = buff.__pos
                
                else:
                    for i in range(data.size):
                        buff.read_char(value[i])

                    buff.set_position(init_pos)
                    buff.read_int(pos_buffer + data.size)

                    pos_buffer = buff.__pos


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