from DBconfig import DBconfig
from PageId import PageId
from Buffer import Buffer

import json


class DiskManager:
    def __init__(self, config:DBconfig):
        self.config = config

        self.current_pageId = None
        self.free_pageIds = []


    def AllocPage(self):
        if(self.free_pageIds != []): 
            return self.free_pageIds.pop(0)

        max_page = self.config.dm_maxfilesize // self.config.pagesize

        # Cas où c'est vide
        if(self.current_pageId is None):
            self.current_pageId = PageId(0, 0)

        # Cas où c'est au milieu d'une page, on incrémente
        elif(self.current_pageId.pageIdx < max_page - 1):
            self.current_pageId = PageId(self.current_pageId.fileIdx, self.current_pageId.pageIdx + 1)
            
        # Cas où c'est la fin d'une page, on change de file
        else:
            self.current_pageId = PageId(self.current_pageId.fileIdx + 1, 0)
        
        return self.current_pageId


    def ReadPage(self, pageId:PageId, buffer:Buffer):
        filename = f"F{pageId.fileIdx}.rsdb"
        pagebyte = self.config.pagesize * pageId.pageIdx

        with open(filename, "rb") as f:
            # Pointer au début de la page
            f.seek(pagebyte, 0) 

            # Lire de la page
            data = f.read(self.config.pagesize)

            # ???
            buffer.from_bytes(data)
            

    def WritePage(self, pageId:PageId, buffer:Buffer):
        filename = f"F{pageId.fileIdx}.rsdb"
        
        pagebyte = self.config.pagesize * pageId.pageIdx

        with open(filename, "wb") as f:
            # Pointer au début de la page
            f.seek(pagebyte, 0)

            # Convertir data en byte
            data = buffer.to_bytes()

            # Ecrire sur le fichier
            f.write(data)


    def DeAllocPage(self, pageId:PageId):
        for pId in self.free_pageIds:
            if pId == pageId:
                raise ValueError(f"la page ({pageId.fileIdx},{pageId.pageIdx}) a déja été desalouée.")
            
        # prendre le cas où pageId < PageId(0, 0)?
        if self.current_pageId < pageId:
            raise ValueError(f"la page ({pageId.fileIdx}, {pageId.pageIdx}) n'existe pas.")
        
        self.free_pageIds.append(pageId)


    def SaveState(self):
        if self.current_pageId is None:
            current_pageId = None

        else:
            current_pageId = self.current_pageId.__dict__
        
        free_pageIds = [page.__dict__ for page in self.free_pageIds]

        data = {"last_created_page": current_pageId,
              "free_pageIds": free_pageIds}
        
        with open("dm.save.json", "w") as f:
            json.dump(data, f, indent= 4)
            
            
    def LoadState(self):
        try:
            with open("dm.save.json", "r") as f:
                data_dict = json.load(f)
            
            self.current_pageId = PageId(**data_dict["last_created_page"]) if data_dict["last_created_page"] is not None else None
            self.free_pageIds = [PageId(**page) for page in data_dict["free_pageIds"]]
       
        except FileNotFoundError:
            print("Le fichier n'a pas été trouvé")



if __name__ == "__main__":
    config = DBconfig.LoadDBConfig("DBconfig.json")
    disk = DiskManager(config)

    disk.AllocPage()
    disk.AllocPage()
    disk.DeAllocPage(PageId(0, 0))
    disk.AllocPage()

    disk.SaveState()