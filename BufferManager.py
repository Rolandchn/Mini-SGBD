from typing import List

from Buffer import Buffer

from PageId import PageId
from DBconfig import DBconfig
from DiskManager import DiskManager



class BufferManager:
    def __init__(self, config:DBconfig, disk:DiskManager):
        self.config = config
        self.disk = disk
        self.used_buffers: List[Buffer] = []

        self.buffers = [Buffer() for i in range(config.bm_buffercount)]
        self.CurrentReplacementPolicy = config.bm_policy[0] #LRU default 


    @staticmethod
    def setup(file):
        # configuration de base
        config = DBconfig.LoadDBConfig(file)
        disk = DiskManager(config)

        # class configuration
        bufferManager = BufferManager(config, disk)
        bufferManager.SetCurrentReplacementPolicy()
        
        return bufferManager 


    def getPage(self, pageId:PageId):
        free_buffer = None

        # cas où il y a une page identique
        for buffer in self.buffers:
            if buffer.pin_count != 0:
                continue
            
            if pageId == buffer.pageId:
                self.used_buffers.append(buffer)
            
                return buffer

            # stock la 1er page libre
            if buffer.pageId is None and free_buffer is None:
                free_buffer = buffer
            
        # cas où il y avait une page libre
        if free_buffer is not None:
            free_buffer.pageId = pageId

            self.used_buffers.append(free_buffer)
            self.disk.ReadPage(pageId,free_buffer)
            return free_buffer

        # pour garder l'ordre d'utilisation ?
        self.used_buffers.append(buffer)

        # cas où toutes les pages sont occupées
        return self.getPageByPolicy(pageId)


    def getPageByPolicy(self, pageId:PageId):
        if self.CurrentReplacementPolicy == "LRU":
            # récupérer le premier buffer utilisé
            buffer = self.used_buffers.pop(0)
        
        elif self.CurrentReplacementPolicy == "MRU":
            # récupérer le dernier buffer utilisé
            buffer = self.used_buffers.pop()
            
        buffer.pageId = pageId
        self.disk.ReadPage(pageId,buffer)
        return buffer

    #Que faire avec les pages dont le dirty est True
    def FreePage(self, pageId:PageId):
        for buffer in self.buffers:
            if pageId == buffer.pageId:
                # réinitialiser les attributs de buffer
                buffer.pageId = None

                # valeur initiale ?
                buffer.dirty_flag = False
                buffer.pin_count -= 1

                break


    def SetCurrentReplacementPolicy(self):
        while True:
            user_input = input("Choose a policy LRU or MRU: ")

            if user_input in self.config.bm_policy:
                self.CurrentReplacementPolicy = user_input
                break
            
            print("This is policy does not exist.")


    def FlushBuffers(self):
        for buffer in self.used_buffers:
            if buffer.dirty_flag:
                self.disk.WritePage(buffer.pageId,buffer)
                buffer.dirty_flag = False
            buffer.pin_count = 0
            buffer.pageId = None
            #azer
if __name__ == "__main__":
    bufferManager = BufferManager.setup("DBconfig.json")
    bufferManager.disk.LoadState()
    print(bufferManager.getPage(PageId(1,1)))
    print(bufferManager.getPage(PageId(1,1)))
    print(bufferManager.getPage(PageId(1,3)))
    print(bufferManager.getPage(PageId(2,1)))
    print(bufferManager.getPage(PageId(15,1)))

    bufferManager.disk.SaveState()


    #Est-on sensé rajouter un read a partir du disque dans le getPageL