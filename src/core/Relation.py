from typing import List
import Column
from PageId import PageId
from Buffer import Buffer
from Record import Record
from DiskManager import DiskManager
from BufferManager import BufferManager
import os
from pathlib import Path

#TODO rajouter des Flag dirty pour les pages modifiées
#TODO liberer tout les buffers

class Relation:
    def __init__(self, name: str, nb_column: int, columns: List[Column.ColumnInfo],
                disk: DiskManager, bufferManager: BufferManager):
        self.name = name
        self.nb_column = nb_column
        self.columns = columns

        self.disk = disk
        self.bufferManager = bufferManager

        self.headerPageId = None

        #Cas ou relation existe pas, allouer une nouvelle page
        if self.headerPageId is None:
            self.headerPageId = self.disk.AllocPage()
            
        buffer = self.bufferManager.getPage(self.headerPageId)
        if(buffer.read_char() == "#"): 
            buffer.set_position(0)
            buffer.put_int(0)
            #remplacer write page par un dirty = true
            bufferManager.disk.WritePage(self.headerPageId, buffer)
            buffer.set_position(0)
        self.bufferManager.disk.SaveState()
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


    def addDataPage(self) -> PageId:
        """ 
        
        """
        buffer = self.bufferManager.getPage(self.headerPageId)
        dataPageId = self.disk.AllocPage()
        # MAJ header page
        buffer.set_position(0)
        n = buffer.read_int()
        buffer.set_position(0)
        buffer.put_int(n + 1)
        buffer.set_position(12 * n + 4)
        buffer.put_int(dataPageId.fileIdx)
        buffer.put_int(dataPageId.pageIdx)
        m = self.disk.config.nb_slots
        pageSize = self.disk.config.pagesize
        buffer.put_int(pageSize - 8 * (m + 1))
        buffer.dirty_flag = True
        #TODO free page au lieu de flushbuffers
        self.bufferManager.FlushBuffers()        
        #Init data page
        buffer2 = self.bufferManager.getPage(dataPageId)
        buffer2.set_position(self.disk.config.pagesize - 8 - 8 * self.disk.config.nb_slots)
        for _ in range(self.disk.config.nb_slots):
            buffer2.put_int(-1)
            buffer2.put_int(0)
            
        buffer2.put_int(self.disk.config.nb_slots)
        buffer2.put_int(0)
        buffer2.set_position(self.disk.config.pagesize - 8 - 8 * self.disk.config.nb_slots)
        
        buffer2.dirty_flag = True
        self.bufferManager.FlushBuffers()

        return dataPageId

    def getFreeDataPageId(self, sizeRecord):
        """ 
         
        """
        buffer = self.bufferManager.getPage(self.headerPageId)
        
        n = buffer.read_int()

        for i in range(n):
            fidx = buffer.read_int()
            pidx = buffer.read_int()
            espaceLibre = buffer.read_int()
            
            if espaceLibre >= sizeRecord:
                return PageId(buffer.read_int(), buffer.read_int())
        
        return None


    def writeRecordToDataPage(self, record: Record, pageId:PageId) -> None:
        """
        Opération: Ecrit le record sur un buffer et met à jour les informations du data page et du header page
        """
        buffer = self.bufferManager.getPage(pageId)
        
        # write record
        # On récupère "position début record disponible" car on ne connait pas quand commence le datapage si il est rempli (si le datapage est vide alors, position est juste 0)
        buffer.set_position(self.disk.config.pagesize - 4) 
        # position début Rec M: pagesize - 4 - 4 - 8 * nb_slots
        positionRecord = buffer.read_int()
        tailleRecord = self.writeRecordToBuffer(record, buffer, positionRecord)
        
        self.updateDataPage(buffer, positionRecord, tailleRecord)
        self.updateHeaderPage(pageId, tailleRecord)

        self.bufferManager.FlushBuffers()


    def updateHeaderPage(self, pageId: PageId, tailleRecord: int):
        buffer = self.bufferManager.getPage(self.headerPageId)
        buffer.set_position(0)
        N = buffer.read_int()
        
        #avancer la position du buffer vers la page souhaitée
        for i in range(N):
            fidx = buffer.read_int()
            pidx = buffer.read_int()
            buffer.set_position(buffer.getPos() + 4)

            if pageId == PageId(fidx, pidx):
                break

        # décrémenter nb octet libre
        buffer.set_position(buffer.getPos() - 4)
        t2 = buffer.read_int()
        buffer.set_position(buffer.getPos() - 4)
        buffer.put_int(t2 - tailleRecord)
        
        buffer.dirty_flag = True
        

    def updateDataPage(self, buffer: Buffer, positionRecord: int, tailleRecord: int):
        # maj rouge
        buffer.set_position(self.disk.config.pagesize - 4)
        buffer.put_int(positionRecord + tailleRecord)

        # maj vert 1
        buffer.set_position(self.disk.config.pagesize - 16)

        while buffer.read_int() != -1:
            buffer.set_position(buffer.getPos() - 12)
        
        else:
            buffer.set_position(buffer.getPos() - 4)
        
        # maj vert 2 
        buffer.put_int(positionRecord)
        buffer.put_int(tailleRecord)

        buffer.dirty_flag = True


    def getRecordsInDataPage(self, pageId: PageId) -> List[Record]:
        """
        Opération: Lis chaque record d'un datapage
        Sortie: Retourne la liste des records d'une datapage
        """
        buffer = self.bufferManager.getPage(pageId)

        # positionner sur le début de la première ligne verte
        buffer.set_position(self.disk.config.pagesize - 16)

        liste = []
        record = Record()

        while(positionRecord := buffer.read_int()) != -1:
            indexRecord = buffer.getPos() - 4
            
            self.readFromBuffer(record, buffer, positionRecord) 
            
            liste.append(record)       
            
            buffer.set_position(indexRecord - 8)

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
            buffer.set_position(buffer.getPos() + 4)
            
        return liste
 
    def InsertRecord(self, record: Record):
        size = self.getRecordSize(record)
        freepage = self.getFreeDataPageId(size)
        if freepage is not None:
            self.writeRecordToDataPage(record, freepage)
        else:
            freepage = self.addDataPage()
            self.writeRecordToDataPage(record, freepage)

    def getRecordSize(self, record: Record) -> int:
        """
        Opération: Calcule la taille totale d'un record
        """
        total_size = 0
        has_varchar = self.has_varchar(self.columns)
        if has_varchar:
            total_size += 4 * (self.nb_column + 1)
        for column, value in zip(self.columns, record.values):
            if isinstance(column.type, Column.Int):
                total_size += 4
            elif isinstance(column.type, Column.Float):
                total_size += 4
            elif isinstance(column.type, Column.Char):
                total_size += column.type.size
            elif isinstance(column.type, Column.VarChar):
                total_size += len(value)
        return total_size

    def GetAllRecords(self):
        liste = self.getDataPages()
        liste2 = []
        for dataPageId in liste:
            liste3 = self.getRecordsInDataPage(dataPageId)
            liste2.append(liste3)

        return liste2
    
    def desallocAllPagesOfRelation(self):
        headerPage = self.bufferManager.getPage(self.headerPageId)
        headerPage.set_position(0)
        nb = headerPage.read_int()
        for _ in range(nb):
            fidx = headerPage.read_int()
            pidx = headerPage.read_int()
            self.disk.DeAllocPage(PageId(fidx, pidx))
            headerPage.set_position(headerPage.getPos() + 4)
        self.disk.DeAllocPage(self.headerPageId)
        
    @classmethod
    def loadRelation(cls, name: str, diskManager, bufferManager, nomBD):
        from pathlib import Path
        import json


        full_path = Path(__file__).parent / ".." / ".." / "storage" / "database" / f"{nomBD}.json"
        full_path = full_path.resolve()

        if not full_path.is_file():
            raise FileNotFoundError(f"fichier introuvable.")

        try:
            # Charger le fichier JSON
            with open(full_path, "r", encoding="utf-8") as db_file:
                data = json.load(db_file)

            # Rechercher la relation
            for relation_data in data:
                if relation_data["name"] == name:
                    columns = []
                    for col_data in relation_data["columns"]:
                        col_type = col_data["type"]["type"]
                        size = col_data["type"]["size"]

                        if col_type == "Int":
                            column_type = Column.Int(size)
                        elif col_type == "Float":
                            column_type = Column.Float(size)
                        elif col_type == "Char":
                            column_type = Column.Char(size)
                        elif col_type == "VarChar":
                            column_type = Column.VarChar(size)
                        else:
                            raise ValueError(f"Type de colonne inconnu : {col_type}")

                        column_info = Column.ColumnInfo(name=col_data["name"], type=column_type)
                        columns.append(column_info)

                    headerPageId = PageId(
                        fileIdx=relation_data["headerPageId"]["fileIdx"],
                        pageIdx=relation_data["headerPageId"]["pageIdx"]
                    )

                    relation = cls.__new__(cls)

                    relation.name = relation_data["name"]
                    relation.nb_column = relation_data["nb_columns"]
                    relation.columns = columns
                    relation.disk = diskManager
                    relation.bufferManager = bufferManager

                    relation.headerPageId = headerPageId

                    return relation

            # Si aucune relation n'est trouvée
            raise ValueError(f"Relation '{name}' non trouvée")

        except json.JSONDecodeError:
            raise ValueError("fichier JSON non conforme")

# lorsqu'on fini avec getDataPages, on doit freePage()
# avant de freePage, on doit save; c'est à dire WritePage() la page qu'on veut free

if __name__ == "__main__":
    # Initialisation du BufferManager et du DiskManager
    bufferManager = BufferManager.setup(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))
    bufferManager.disk.LoadState()

    # Création d'une nouvelle relation
    columns = [Column.ColumnInfo("id", Column.Int()), Column.ColumnInfo("name", Column.VarChar(50))]
    relation = Relation("TestTable", len(columns), columns, bufferManager.disk, bufferManager)

    # Vérification de l'initialisation de la page d'en-tête
    print(relation.headerPageId)
    n = bufferManager.getPage(relation.headerPageId)
    nb = n.read_int()
    print(nb)

    # Ajout de quelques pages de données
    for _ in range(5):
        relation.addDataPage()

    # Tester la méthode getFreeDataPageId
    record = Record([1, "Alice"])
    size = relation.getRecordSize(record)
    free_page_id = relation.getFreeDataPageId(size)
    if free_page_id:
        print(f"Free page found: {free_page_id}")
    else:
        print("No free page found with sufficient space.")

    # Sauvegarde de l'état
    bufferManager.disk.SaveState()
