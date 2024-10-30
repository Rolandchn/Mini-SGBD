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
        has_varchar = self.has_varchar(self.columns)

        if has_varchar:
            self.put_offset_to_buffer(buff.__pos + 4 * (self.nb_column + 1), pos, buff)
            adress_pos = pos + 4
        
        else:
            buff.set_position(pos)
<<<<<<< HEAD

        # record = single row of values 
        for value, value_info in zip(record.values, self.columns):
            self.put_value_to_buffer(value, value_info, buff)

            if has_varchar:
                self.put_offset_to_buffer(buff.__pos, adress_pos, buff)
                adress_pos += 4
=======

        # record = single row of values 
        for value, value_info in zip(record.values, self.columns):
            self.put_value_to_buffer(value, value_info, buff)

            if has_varchar:
                self.put_offset_to_buffer(buff.__pos, adress_pos, buff)
                adress_pos += 4
        
        return adress_pos - pos


    @staticmethod
    def put_offset_to_buffer(value_pos, adress_pos, buff: Buffer):
        buff.set_position(adress_pos)
        buff.put_int(value_pos)

        buff.set_position(value_pos)


    @staticmethod
    def put_value_to_buffer(value, value_info: Column.ColumnInfo, buff: Buffer):
        if isinstance(value_info.type, Column.Number):
            if type(value) == float:
                buff.put_float(float(value))
            
            else:
                buff.put_int(int(value))

        elif isinstance(value_info.type, Column.Char):
            for char in value:
                buff.put_char(char)


    def readFromBuffer(self, record, buff, pos) -> int:
        buff.set_position(pos)
>>>>>>> main
        
        return adress_pos - pos


<<<<<<< HEAD
    @staticmethod
    def put_offset_to_buffer(value_pos, adress_pos, buff: Buffer):
        buff.set_position(adress_pos)
        buff.put_int(value_pos)

        buff.set_position(value_pos)


    @staticmethod
    def put_value_to_buffer(value, value_info: Column.ColumnInfo, buff: Buffer):
        if isinstance(value_info.type, Column.Number):
            if type(value) == float:
                buff.put_float(float(value))
            
            else:
                buff.put_int(int(value))

        elif isinstance(value_info.type, Column.Char):
            for char in value:
                buff.put_char(char)
=======
        else:
            value_pos = buff.read_int()
            
            buff.put_int(value_pos)
            adress_pos = buff.__pos

            for index, value in enumerate(record.values):
                buff.set_position(value_pos)
>>>>>>> main


    def readFromBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        has_varchar = self.has_varchar(self.columns)

<<<<<<< HEAD
        if has_varchar:
            self.put_offset_to_buffer(buff.__pos + 4 * (self.nb_column + 1), pos, buff)
            adress_pos = pos + 4
        
        else:
            buff.set_position(pos)

        # record = single row of values 
        for value_info in self.columns:
            value = self.read_value_from_buffer(value_info, buff)

            if has_varchar:
                self.put_offset_to_buffer(buff.__pos, adress_pos, buff)
                adress_pos += 4
        
        return adress_pos - pos
    
    
    def read_value_from_buffer(value_info: Column.ColumnInfo, buff: Buffer) -> int | float | str:
        if isinstance(value_info.type, Column.Number):
            buff.read_float(float(value))
            
            else:
                buff.read_int(int(value))
=======
                    buff.set_position(adress_pos)
                    buff.read_int(value_pos + 4)

                    value_pos = buff.__pos + 4

                
                elif isinstance(data, Column.Char):
                    for i in range(data.size):
                        buff.put_char(value[i])

                    buff.set_position(adress_pos)
                    buff.read_int(value_pos + data.size)

                    value_pos = buff.__pos
                
                else:
                    for i in range(data.size):
                        buff.read_char(value[i])

                    buff.set_position(adress_pos)
                    buff.read_int(value_pos + data.size)

                    value_pos = buff.__pos
>>>>>>> main

        elif isinstance(value_info.type, Column.Char):
            for char in value:
                buff.read_char(char)

    @staticmethod
    def has_varchar(columns):
        for stuff in columns:
            if isinstance(stuff, Column.CharVar):
                return True
            
        return False
