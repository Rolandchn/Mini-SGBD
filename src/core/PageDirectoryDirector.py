from typing import List, Optional, Tuple
from IRecordIterator import IRecordIterator
from Record import Record
from Relation import Relation
from PageId import PageId
from Condition import Condition
from BufferManager import BufferManager
from DiskManager import DiskManager
from Buffer import Buffer

class PageDirectoryIterator:
    def __init__(self, relation: Relation, disk_manager: DiskManager, buffer_manager: BufferManager):
        """
        Initialise l'itérateur de répertoire de pages pour une relation donnée
        
        :param relation: La relation dont on veut parcourir les pages
        :param disk_manager: Le gestionnaire de disque
        :param buffer_manager: Le gestionnaire de tampons
        """
        self.relation = relation
        self.disk_manager = disk_manager
        self.buffer_manager = buffer_manager
        
        # Récupère la page d'en-tête 
        self.header_page_buffer = self.buffer_manager.getPage(relation.headerPageId)
        
        # Initialise l'index de page
        self.current_page_index = 0
        
    def GetNextDataPageId(self) -> Optional[PageId]:
        """
        Retourne l'identifiant de la prochaine page de données
        
        :return: Un PageId ou None si plus de pages
        """
        # Récupère le nombre total de pages de données
        total_data_pages = int.from_bytes(
            self.header_page_buffer.data[:4], 
            byteorder='little'
        )
        
        # Vérifie si on a encore des pages à parcourir
        if self.current_page_index < total_data_pages:
            # Calcule le PageId de la page de données courante
            data_page_id = PageId(
                self.relation.file_idx, 
                self.current_page_index + 1  # +1 car l'index 0 est la page d'en-tête
            )
            
            # Incrémente l'index pour la prochaine itération
            self.current_page_index += 1
            
            return data_page_id
        
        # Libère la page d'en-tête
        self.buffer_manager.FreePage(self.relation.headerPageId)
        
        return None
    
    def Close(self):
        """
        Libère la page d'en-tête si ce n'est pas déjà fait
        """
        if self.header_page_buffer:
            self.buffer_manager.FreePage(self.relation.headerPageId)
            self.header_page_buffer = None
