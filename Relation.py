from typing import List
import Column

from PageId import PageId
from Buffer import Buffer
from Record import Record
from DiskManager import DiskManager
from BufferManager import BufferManager

class Relation:
    def __init__(self, name: str, nb_column: int, columns: List[Column.ColumnInfo],
                disk: DiskManager, bufferManager: BufferManager):
        self.name = name
        self.nb_column = nb_column
        self.columns = columns

        self.disk = disk
        self.bufferManager = bufferManager
    
        #init
        self.headerPageId = disk.AllocPage()
        buffer = self.bufferManager.getPage(self.headerPageId)
        buffer.put_int(0)
    

    def writeRecordToBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        """ 
        Opération: Itère dans le Record puis écrit dans le buffer, si il y a un varchar on enregistre l'adresse de début et de fin de chaque valeurs dans le offset. 
        Sortie: Le nombre d'octet traité par l'écriture
        """
        has_varchar = self.has_varchar(self.columns)

        if has_varchar:
            self.put_offset_to_buffer(pos, buff.__pos + 4 * (self.nb_column + 1), buff)
            adress_pos = pos + 4
        
        else:
            buff.set_position(pos)

        # record = single row of values 
        for value, value_info in zip(record.values, self.columns):
            self.put_value_to_buffer(value, value_info, buff)

            if has_varchar:
                self.put_offset_to_buffer(adress_pos, buff.__pos, buff)
                adress_pos += 4
        
        return adress_pos - pos


    @staticmethod
    def put_offset_to_buffer(adress_pos, value_pos, buff: Buffer) -> None:
        """ 
        Opération: Ecrit l'adresse de la valeur dans le buffer sur l'emplacement offset puis pointe à la position initiale
        """
        buff.set_position(adress_pos)
        buff.put_int(value_pos)

        buff.set_position(value_pos)


    @staticmethod
    def put_value_to_buffer(value, value_info: Column.ColumnInfo, buff: Buffer):
        """ 
        Opération: Ecrit la valeur dans le buffer 
        """
        if isinstance(value_info.type, Column.Number):
            if type(value) == float:
                buff.put_float(float(value))
            
            else:
                buff.put_int(int(value))

        elif isinstance(value_info.type, Column.Char):
            for char in value:
                buff.put_char(char)


    def readFromBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        """ 
        Opération: Itère dans le buffer puis enregistre les valeurs dans le record, si il y a un varchar on lis l'adresse de début et de fin de chaque valeurs dans le offset.
        Sortie: Retourne le nombre d'octet traité par la lecture
        """

        # adress_pos = adress of the index in the offset
        # value_pos = adress of the value in the offset 
        has_varchar = self.has_varchar(self.columns)

        if has_varchar:
            adress_pos = pos + 4
            # value_pos = buff.__pos
            next_value_pos = self.read_offset_from_buffer(adress_pos, buff.__pos + 4 * (self.nb_column + 1), buff)
        
        else:
            buff.set_position(pos)

        # record = single row of values 
        for value_info in self.columns:
            if has_varchar:
                record.values.append(self.read_value_from_buffer(value_info, next_value_pos - buff.__pos, buff))

                adress_pos += 4
                # get the value from adress_pos, and go back to the last buffer position
                next_value_pos = self.read_offset_from_buffer(adress_pos, buff.__pos, buff)
            
            else:
                record.values.append(self.read_value_from_buffer(value_info, value_info.type.size, buff))
    
        if has_varchar: return next_value_pos
        return buff.__pos - pos
    
    
    @staticmethod
    def read_value_from_buffer(value_info: Column.ColumnInfo, value_size: int, buff: Buffer)  -> int | float | str:
        """ 
        Opération: Lis une valeur du buffer
        """
        if isinstance(value_info.type, Column.Number):
            if value_info.type == float:
                return buff.read_float()
    
            return buff.read_int()
        
        value = ""

        for i in range(value_size):
            value += buff.read_char()

        return value


    @staticmethod
    def read_offset_from_buffer(adress_pos, value_pos, buff: Buffer):
        """ 
        Opération: Lis l'adresse d'une valeur dans le buffer sur l'emplacement offset puis pointe à la position initiale
        """
        buff.set_position(adress_pos)
        next_value_pos = buff.read_int()

        buff.set_position(value_pos)

        return next_value_pos


    @staticmethod
    def has_varchar(columns) -> bool:
        """ 
        Opération: Vérifie si la colonne contient un varchar
        """

        for stuff in columns:
            if isinstance(stuff, Column.Char) and stuff.var:
                return True
            
        return False


    def addDataPage(self) -> None:
        """ 
          
        """
        dataPageId = self.disk.AllocPage()
        buffer = self.bufferManager.getPage(dataPageId)
        
        n = buffer.read_int()
        buffer.set_position(12 * n)

        buffer.put_int(dataPageId.fileIdx)
        buffer.put_int(dataPageId.pageIdx)
        
        m = self.disk.config.nb_slot
        pageSize = self.disk.config.pagesize
        buffer.put_int(pageSize - 8 * (m + 1))

        buffer.set_position(0)
        buffer.put_int(n + 1)


    def getFreeDataPageId(self, sizeRecord):
        """ 
         
        """
        buffer = self.bufferManager.getPage(self.headerPageId)
        
        n = buffer.read_int()

        for i in range(n):
            buffer.set_position((i + 1) * 12)
            position = buffer.read_int()
            
            if position <= sizeRecord:
                pos = position - 8
                buffer.set_position(pos)

                return PageId(buffer.read_int(), buffer.read_int())
        
        return None
        
    def writeRecordToDataPage(self, record: Record, pageId:PageId):
        pass
        # position début Rec M: pagesize - 4 - 4 - 8 * nb_slot
        # position début espace disponible = somme de toutes les tailles

# lorsqu'on fini avec getDataPages, on doit freePage()
# avant de freePage, on doit save; c'est à dire WritePage() la page qu'on veut free