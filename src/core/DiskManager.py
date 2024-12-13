from DBconfig import DBconfig
from PageId import PageId
from Buffer import Buffer

import json
import os

current_dir = os.path.dirname(__file__)

config_file = os.path.join(current_dir, "..", "config", "DBconfig.json")

savefile = os.path.join(current_dir, "..", "config", "dm.save.json")

config = DBconfig.LoadDBConfig(config_file) 


dbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", config.dbpath, "datafiles"))
os.makedirs(dbpath, exist_ok=True)

class DiskManager:
    def __init__(self, config:DBconfig):
        self.config = config
        self.current_pageId = None
        self.free_pageIds = []


    def AllocPage(self):
        """ 
        Alloue une nouvelle page et la remplit avec exactement `pagesize` caractères `#`.
        """
        freePageId = None
        max_page = self.config.dm_maxfilesize // self.config.pagesize

        if self.free_pageIds:
            freePageId = self.free_pageIds.pop(0)

        elif self.current_pageId is None:
            self.current_pageId = PageId(0, 0)
            freePageId = self.current_pageId

        elif self.current_pageId.pageIdx < max_page - 1:
            self.current_pageId = PageId(self.current_pageId.fileIdx, self.current_pageId.pageIdx + 1)
            freePageId = self.current_pageId

        else:
            self.current_pageId = PageId(self.current_pageId.fileIdx + 1, 0)
            freePageId = self.current_pageId

        filename = os.path.join(dbpath, f"F{freePageId.fileIdx}.rsdb")
        page_start_byte = self.config.pagesize * freePageId.pageIdx

        # Création du fichier si nécessaire
        if not os.path.isfile(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                pass

        # Remplissage de la page allouée avec des #
        with open(filename, "r+b") as f:
            f.seek(page_start_byte, os.SEEK_SET)

            f.write(b"#" * self.config.pagesize)
        

        return freePageId




    def ReadPage(self, pageId:PageId, buffer:Buffer) -> None:
        """ 
        Opération: Lis le buffer  
        """
        filename = os.path.join(dbpath, f"F{pageId.fileIdx}.rsdb")
        pagebyte = self.config.pagesize * pageId.pageIdx

        with open(filename, "rb") as f:
            # Pointer au début de la page
            f.seek(pagebyte, 0) 

            # Lire de la page
            data = f.read(self.config.pagesize)

            buffer.from_bytes(data)
            

    def WritePage(self, pageId:PageId, buffer:Buffer) -> None:
        """ 
        Opération: Ecrit le buffer dans un fichier de donnée.
        """
        filename = os.path.join(dbpath, f"F{pageId.fileIdx}.rsdb")
        
        pagebyte = self.config.pagesize * pageId.pageIdx
        with open(filename, "r+b") as f:
            # Pointer au début de la page
            f.seek(pagebyte, 0)

            # Convertir data en byte
            data = buffer.to_bytes()

            # Ecrire sur le fichier
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    def DeAllocPage(self, pageId:PageId) -> None:
        """
        Opération: Ajoute le pageId dans la liste de pageId réutilisable
        """
        try:
            for pId in self.free_pageIds:
                if pId == pageId:
                    raise ValueError(f"la page ({pageId.fileIdx},{pageId.pageIdx}) a déja été desalouée.")
                
            # prendre le cas où pageId < PageId(0, 0)?
            if self.current_pageId < pageId:
                raise ValueError(f"la page ({pageId.fileIdx}, {pageId.pageIdx}) n'existe pas.")
            
            self.free_pageIds.append(pageId)
        except ValueError as e:
            print(e)


    def SaveState(self) -> None:
        """
        Opération: Sauvegarde toutes les données, sous forme de dictionnaire, dans le fichier dm.save.json
        """
        if self.current_pageId is None:
            current_pageId = None

        else:
            current_pageId = self.current_pageId.__dict__
        
        free_pageIds = [page.__dict__ for page in self.free_pageIds]

        data = {"last_created_page": current_pageId,
              "free_pageIds": free_pageIds}
        
        with open(savefile, "w") as f:
            json.dump(data, f, indent= 4)
            
            
    def LoadState(self):
        """ 
        Opération: Charge toutes les données à partir du fichier dm.save.json
        """
        try:
            with open(savefile, "r") as f:
                data_dict = json.load(f)
            
            self.current_pageId = PageId(**data_dict["last_created_page"]) if data_dict["last_created_page"] is not None else None
            self.free_pageIds = [PageId(**page) for page in data_dict["free_pageIds"]]
       
        except FileNotFoundError:
            print("Le fichier n'a pas été trouvé")


if __name__ == "__main__":
    config = DBconfig.LoadDBConfig(config_file)
    disk = DiskManager(config)
    buff = Buffer()    
    disk.SaveState()
    
    
    