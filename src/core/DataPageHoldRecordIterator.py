from typing import Optional
from Record import Record
from Relation import Relation
from PageId import PageId
from BufferManager import BufferManager

class DataPageHoldRecordIterator:
    def __init__(self, relation: Relation, page_id: PageId, buffer_manager: BufferManager):
        """
        Initialise l'itérateur de records pour une page de données
        
        :param relation: La relation contenant les données
        :param page_id: L'identifiant de la page à parcourir
        :param buffer_manager: Le gestionnaire de tampons
        """
        self.relation = relation
        self.page_id = page_id
        self.buffer_manager = buffer_manager
        
        # Récupère le buffer pour la page
        self.page_buffer = self.buffer_manager.getPage(page_id)
        
        # Initialise l'index du record
        self.current_record_index = 0
        
        # Récupère le nombre de records dans la page
        self.total_records = int.from_bytes(
            self.page_buffer.data[4:8], 
            byteorder='little'
        )
    
    def GetNextRecord(self) -> Optional[Record]:
        """
        Retourne le prochain record de la page
        
        :return: Un Record ou None si plus de records
        """
        if self.current_record_index < self.total_records:
            record_offset = 8 + (self.current_record_index * self.relation.record_size)
            record_data = self.page_buffer.data[
                record_offset : record_offset + self.relation.record_size
            ]
            
            # Convertit les données brutes en valeurs de record
            record_values = self.relation.parse_record_data(record_data)
            
            self.current_record_index += 1
            
            return Record(record_values)
        
        return None
    
    def Reset(self):
        """
        Réinitialise l'itérateur au début de la page
        """
        self.current_record_index = 0
    
    def Close(self):
        """
        Libère le buffer de la page
        """
        if self.page_buffer:
            self.buffer_manager.FreePage(self.page_id)
            self.page_buffer = None
