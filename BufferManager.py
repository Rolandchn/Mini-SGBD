from typing import List

from Buffer import Buffer

from PageId import PageId
from DBconfig import DBconfig
from DiskManager import DiskManager



class BufferManager:
    def __init__(self, config:DBconfig, disk:DiskManager):
        self.config = config
        self.disk = disk
        self.free_buffers: List[Buffer] = []

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


    def getPage(self, pageId:PageId) -> Buffer:
        """
        Opération: Retourne un buffer libre ou buffer du même pageId ou buffer remplacé
        Sortie: Buffer
        """
        free_buffer = None
        has_pin_count0 = False

        # cas où il y a une page identique
        for buffer in self.buffers:
            if buffer.pin_count != 0:
                continue
            else:
                has_pin_count0 = True
            
            if pageId == buffer.pageId:
                buffer.pin_count += 1
                return buffer

            # stock la 1er page libre
            if buffer.pageId is None and free_buffer is None:
                free_buffer = buffer
            
        # cas où il y avait une page libre
        if free_buffer is not None:
            free_buffer.pageId = pageId

            self.disk.ReadPage(pageId,free_buffer)
            free_buffer.pin_count += 1

            return free_buffer

        # cas où toutes les pages sont occupées
        if has_pin_count0:
            return self.getPageByPolicy(pageId)
        else:
            print("erreur fatal - aucun buffer disponible")


    def getPageByPolicy(self, pageId:PageId) -> Buffer:
        """
        Opération: Retourne un buffer parmis les buffers inactifs (pin_count = 0) et sauvegarde le buffer s'il a déjà été utilisé (dirty_flag = True)
        Sortie: Buffer libre
        """

        if self.CurrentReplacementPolicy == "LRU":
            # récupérer le premier buffer utilisé
            buffer = self.free_buffers.pop(0)
        
        elif self.CurrentReplacementPolicy == "MRU":
            # récupérer le dernier buffer utilisé
            buffer = self.free_buffers.pop()
        
        if buffer.dirty_flag:
            self.disk.WritePage(buffer.pageId, buffer)

        buffer.pageId = pageId
        
        self.disk.ReadPage(pageId,buffer)

        buffer.pin_count += 1

        return buffer


    def FreePage(self, pageId:PageId) -> None:
        """
        Opération: Décremente de pin_count d'un buffer grâce au pageId
        """
        for buffer in self.buffers:
            if pageId == buffer.pageId:
                # valeur initiale 0 car on utilise qu'un seul processus
                buffer.pin_count -= 1

                self.free_buffers.append(buffer)

                break


    def SetCurrentReplacementPolicy(self) -> None:
        """
        Opération: Demande à l'utilisateur la politique de remplacement
        """
        while True:
            user_input = input("Choose a policy LRU or MRU: ")

            if user_input in self.config.bm_policy:
                self.CurrentReplacementPolicy = user_input
                break
            
            print("This is policy does not exist.")


    def FlushBuffers(self) -> None:
        """
        Opération: Enregistre tous les buffers utilisés (dirty_flag = True) et les vide
        """
        for buffer in self.buffers:
            if buffer.dirty_flag:
                self.disk.WritePage(buffer.pageId,buffer)
                buffer.dirty_flag = False
            buffer.pin_count = 0
            buffer.pageId = None


if __name__ == "__main__":
    bufferManager = BufferManager.setup("DBconfig.json")
    bufferManager.disk.LoadState()

    buff = bufferManager.getPage(PageId(2, 1))
    print(buff.read_int())
    print(buff.read_char())
    




    #Est-on sensé rajouter un read a partir du disque dans le getPageL