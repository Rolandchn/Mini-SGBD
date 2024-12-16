from typing import List
import os
from Buffer import Buffer

from PageId import PageId
from DBconfig import DBconfig
from DiskManager import DiskManager

class BufferManager:
    def __init__(self, config:DBconfig, disk:DiskManager):
        self.config = config
        self.disk = disk
        self.free_buffers: List[Buffer] = []

        self.buffers = [Buffer(size=config.pagesize) for _ in range(config.bm_buffercount)]

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
            if pageId == buffer.pageId:
                buffer.pin_count = 1

                if buffer in self.free_buffers:
                    self.free_buffers.remove(buffer)

                return buffer
            
            if buffer.pin_count != 0:
                continue

            else:
                has_pin_count0 = True
            
            # stock la 1er page libre (cas où il y a des buffers vide, tout début du programme)
            if buffer.pageId is None and free_buffer is None:
                free_buffer = buffer
            
        # cas où il y avait une page libre (cas où il y a des buffers vide, tout début du programme)
        if free_buffer is not None:
            free_buffer.pageId = pageId

            self.disk.ReadPage(pageId,free_buffer)
            free_buffer.pin_count = 1

            return free_buffer

        # cas où il y a des buffers innocupés
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
            buffer.dirty_flag = False

        buffer.pageId = pageId
        
        self.disk.ReadPage(pageId, buffer)

        buffer.pin_count = 1

        return buffer


    def FreePage(self, pageId:PageId) -> None:
        """
        Opération: Décremente de pin_count d'un buffer grâce au pageId
        """
        for buffer in self.buffers:
            if pageId == buffer.pageId:
                # valeur initiale 0 car on utilise qu'un seul processus
                buffer.pin_count = 0

                if buffer not in self.free_buffers:
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

    def afficherBufferManager(self):
        
        print("\nBufferManager: ")
        print("==================================")
        print("buffers:")
        for buffer in self.buffers:
            print(buffer)

        print("---------------------------------")

        print("free buffer:")
        for buffer in self.free_buffers:
            print(buffer)
        
        print("==================================")
