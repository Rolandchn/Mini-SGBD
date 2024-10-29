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
                    for char in value:
                        buff.put_char(char)

        else:
            pos_buffer_adress = buff.__pos
            pos_buffer_value = buff.__pos + 4 * (self.nb_column + 1)
            
            buff.put_int(pos_buffer_value)
            pos_buffer_adress += 4

            for value, value_info in zip(record.values, self.columns):
                buff.set_position(pos_buffer_value)

                if isinstance(value_info.type, Column.Number):
                    if type(value) == float:
                        buff.put_float(float(value))
                    
                    else:
                        buff.put_int(int(value))

                elif isinstance(value_info.type, Column.Char):
                    for char in value:
                        buff.put_char(char)

                else:
                    for char in value:
                        buff.put_char(char)

                buff.set_position(pos_buffer_adress)
                buff.put_int(pos_buffer_value + value_info.type.size_var)

                pos_buffer_value = buff.__pos


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
            pos_buffer_value = buff.read_int()
            
            buff.put_int(pos_buffer_value)
            pos_buffer_adress = buff.__pos

            for index, value in enumerate(record.values):
                buff.set_position(pos_buffer_value)

                data = self.columns[index]

                if isinstance(data, Column.Number):
                    if type(data.value) == float:
                        buff.read_float(float(value))
                    
                    else:
                        buff.read_int(int(value))

                    buff.set_position(pos_buffer_adress)
                    buff.read_int(pos_buffer_value + 4)

                    pos_buffer_value = buff.__pos + 4

                
                elif isinstance(data, Column.Char):
                    for i in range(data.size):
                        buff.put_char(value[i])

                    buff.set_position(pos_buffer_adress)
                    buff.read_int(pos_buffer_value + data.size)

                    pos_buffer_value = buff.__pos
                
                else:
                    for i in range(data.size):
                        buff.read_char(value[i])

                    buff.set_position(pos_buffer_adress)
                    buff.read_int(pos_buffer_value + data.size)

                    pos_buffer_value = buff.__pos


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