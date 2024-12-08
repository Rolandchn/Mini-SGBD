import os
from typing import List
import Column
from pathlib import Path
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
        #init header page
        script_dir = Path(__file__).parent
        try:
            file_path = script_dir / "../../storage/F0.rsdb"
            file_path.resolve()
            self.headerPageId = PageId(0, 0) if file_path.is_file() else disk.AllocPage()
        except Exception as e:
            print("Erreur :", e)
        buffer = self.bufferManager.getPage(self.headerPageId)
        if(buffer.read_char() == "#"): 
            buffer.set_position(0)
            buffer.put_int(0)
            buffer.dirty_flag = True
    def writeRecordToBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        """ 
        Opération: Itère dans le Record puis écrit dans le buffer, si il y a un varchar on enregistre l'adresse de début et de fin de chaque valeurs dans le offset. 
        Sortie: Le nombre d'octet traité par l'écriture
        """
        
        has_varchar = self.has_varchar(self.columns)

        if has_varchar:
            adress_pos = pos

            self.put_offset_to_buffer(pos, pos + 4 * (self.nb_column + 1), buff)

            adress_pos = pos + 4
        
        else:
            buff.set_position(pos)

        # record = single row of values 
        for value, column in zip(record.values, self.columns):
            self.put_value_to_buffer(value, column, buff)

            if has_varchar:
                self.put_offset_to_buffer(adress_pos, buff.getPos(), buff)
                adress_pos += 4
        
        return buff.getPos() - pos


    @staticmethod
    def put_offset_to_buffer(adress_pos, value_pos, buff: Buffer) -> None:
        """ 
        Opération: Ecrit l'adresse de la valeur dans le buffer sur l'emplacement offset puis pointe à la position initiale
        """
        buff.set_position(adress_pos)
        buff.put_int(value_pos)

        buff.set_position(value_pos)


    @staticmethod
    def put_value_to_buffer(value, column: Column.ColumnInfo, buff: Buffer):
        """ 
        Opération: Ecrit la valeur dans le buffer 
        """
        if isinstance(column.type, Column.Int):
            buff.put_int(int(value))
            return 4


        elif isinstance(column.type, Column.Float):
            buff.put_float(float(value))
            return 4
        
        elif isinstance(column.type, Column.Char):
            for index in range(column.type.size):
                buff.put_char(value[index])

            return index

        for index in range(len(value)):
            buff.put_char(value[index])

        return index
        

    def readFromBuffer(self, record: Record, buff: Buffer, pos: int) -> int:
        """ 
        Opération: Itère dans le buffer puis enregistre les valeurs dans le record, si il y a un varchar on lis l'adresse de début et de fin de chaque valeurs dans le offset.
        Sortie: Retourne le nombre d'octet traité par la lecture
        """

        # adress_pos = adress of the index in the offset
        # value_pos = adress of the value 
        
        value_size = 0
        
        has_varchar = self.has_varchar(self.columns)

        if has_varchar:
            adress_pos = pos
            
        else:
            buff.set_position(pos)

        # record = single row of values 
        for value_info in self.columns:
            if has_varchar:
                value_pos = self.read_offset_from_buffer(adress_pos, buff)

                adress_pos += 4

                value_size = self.read_offset_from_buffer(adress_pos, buff) - value_pos

                buff.set_position(value_pos)
            
            value = self.read_value_from_buffer(value_info, value_size, buff)
            record.values.append(value)
    
        return buff.getPos() - pos
    
    
    @staticmethod
    def read_value_from_buffer(value_info: Column.ColumnInfo, value_size: int, buff: Buffer)  -> int | float | str:
        """ 
        Opération: Lis une valeur du buffer
        """
        if isinstance(value_info.type, Column.Int):
            return buff.read_int()
        
        elif isinstance(value_info.type, Column.Float):
            return buff.read_float()
        
        elif isinstance(value_info.type, Column.Char):
            value = ""

            for i in range(value_info.type.size):
                value += buff.read_char()

            return value
        
        # cas VarChar
        value = ""

        for i in range(value_size):
            value += buff.read_char()

        return value


    @staticmethod
    def read_offset_from_buffer(adress_pos, buff: Buffer):
        """ 
        Opération: Lis l'adresse d'une valeur dans le buffer sur l'emplacement offset puis pointe à la position initiale
        """
        buff.set_position(adress_pos)

        value_pos = buff.read_int()

        buff.set_position(value_pos)

        return value_pos


    @staticmethod
    def has_varchar(columns:List[Column.ColumnInfo]) -> bool:
        """ 
        Opération: Vérifie si la colonne contient un varchar
        """

        for column in columns:
            if isinstance(column.type, Column.VarChar):
                return True
            
        return False


    def addDataPage(self) -> None:
        """ 
        
        """
        dataPageId = self.disk.AllocPage()
        buffer = self.bufferManager.getPage(self.headerPageId)
        # MAJ header page
        try:
            n = buffer.read_int()
            buffer.set_position(0)
            buffer.put_int(n + 1)
            buffer.set_position(12 * n + 4)

            buffer.put_int(dataPageId.fileIdx)
            buffer.put_int(dataPageId.pageIdx)
            
            m = self.disk.config.nb_slot
            pageSize = self.disk.config.pagesize
            buffer.put_int(pageSize - 8 * (m + 1))

        except Exception as e:
            print("Erreur :", e)

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
        buffer = self.bufferManager.getPage(self.headerPageId)
        buffer.set_position(0)
        N = buffer.read_int()

        for i in range(N):
            fidx = buffer.read_int()
            pidx = buffer.read_int()

            if pageId == PageId(fidx, pidx):
                break

        # page vide, init data page
        if buffer.read_int() == self.disk.config.pagesize:
            buffer2 = self.bufferManager.getPage(pageId)
            buffer2.set_position(self.disk.config.pagesize - 8 - 8 * self.disk.config.nb_slot)
            
            for i in range(self.disk.config.nb_slot):
                buffer2.put_int(-1)
                buffer2.put_int(0)
            
            buffer2.put_int(self.disk.config.nb_slot)
            buffer2.put_int(0)
            buffer.set_position(buffer.__pos - 4)
            buffer.put_int(self.disk.config.pagesize - 8 - 8 * self.disk.config.nb_slot)

        # write record
        buffer2.set_position(self.disk.config.pagesize - 4)
        position_debut = buffer2.read_int()
        tailleRecord = self.writeRecordToBuffer(record, buffer2, position_debut)

        # maj rouge
        buffer2.set_position(self.disk.config.pagesize - 4)
        buffer2.put_int(position_debut + tailleRecord)

        buffer2.set_position(self.disk.config.pagesize - 16)
        
        # maj vert 1
        while buffer2.read_int() != -1:
            buffer2.set_position(buffer.__pos - 12)
        
        # maj vert 2 
        buffer2.put_int(position_debut)
        buffer2.put_int(tailleRecord)

        # décrémenter nb octet libre
        buffer.set_position(buffer.__pos - 4)
        t2 = buffer.read_int()
        buffer.set_position(buffer.__pos - 4)
        buffer.put_int(t2 - tailleRecord)

        # incrementer jaune
        buffer.set_position(0)
        t3 = buffer.read_int() + 1
        buffer.set_position(0)
        buffer.put_int(t3)
            

        # position début Rec M: pagesize - 4 - 4 - 8 * nb_slot
        # position début espace disponible = somme de toutes les tailles


    def getRecordsInDataPage(self, pageId: PageId):
        buffer = self.bufferManager.getPage(pageId)

        buffer.set_position(self.disk.config.pagesize - 16)

        liste = []

        while buffer.read_int() != -1:
            buffer.set_position(buffer.__pos - 4)
            record = Record([])
            self.readFromBuffer(record, buffer, buffer.read_int()) 
            liste.append(record)       
            buffer.set_position(buffer.__pos - 12)

        return liste
    
    def getDataPages(self):
        buffer = self.bufferManager.getPage(self.headerPageId)
        buffer.set_position(0)
        N = buffer.read_int()

        liste = []

        for i in range(N):
            fidx = buffer.read_int()
            pidx = buffer.read_int()

            liste.append(PageId(fidx,pidx))
            buffer.set_position(buffer.__pos + 4)

        return liste
    
    def InsertRecord(self, record):
        buffer = self.bufferManager.getPage(self.headerPageId)
        buffer.set_position(0)
        N = buffer.read_int()

        liste = []

        for i in range(N):
            fidx = buffer.read_int()
            pidx = buffer.read_int()

        ...

    def GetAllRecords(self):
        liste = self.getDataPages()
        liste2 = []
        for dataPageId in liste:
            liste3 = self.getRecordsInDataPage(dataPageId)
            liste2.append(liste3)

        return liste2

# lorsqu'on fini avec getDataPages, on doit freePage()
# avant de freePage, on doit save; c'est à dire WritePage() la page qu'on veut free

if __name__ == "__main__":
    bufferManager = BufferManager.setup(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))

    liste = [Column.ColumnInfo("test1", Column.Char(3)), Column.ColumnInfo("test2", Column.Int())]

    relation = Relation("test", 2, liste, bufferManager.disk, bufferManager) 
    
    record1 = Record(["azt", 2])

    buff = bufferManager.getPage(PageId(0, 0))

    op1 = relation.writeRecordToBuffer(record1, buff, 0)

    buff.set_position(0)

    record2 = Record([])

    op2 = relation.readFromBuffer(record2, buff, 0)

    for x in record2.values:
        print(x)

    print(op2)
    