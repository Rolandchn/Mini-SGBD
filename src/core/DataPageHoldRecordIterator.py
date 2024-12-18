from typing import Optional
from Record import Record
from Relation import Relation
from PageId import PageId
from BufferManager import BufferManager

class DataPageHoldRecordIterator:
    def __init__(self, relation: Relation, page_id: PageId):
        """
        Initialise l'itérateur de records pour une page de données

        :param relation: La relation contenant les données
        :param page_id: L'identifiant de la page à parcourir
        """
        self.relation = relation
        self.page_id = page_id
        self.buffer_manager = relation.bufferManager

        self.page_buffer = self.buffer_manager.getPage(page_id)
        print(self.page_buffer)
        print
        if self.page_buffer is None:
            raise ValueError(f"Impossible de récupérer la page {page_id}")

        # Positionner à la fin de la page pour lire le nombre de slots
        self.page_buffer.set_position(self.buffer_manager.disk.config.pagesize - 8)
        nbslots = self.page_buffer.read_int()

        self.total_records = 0
        self.page_buffer.set_position(self.buffer_manager.disk.config.pagesize - 8)

        # Compter le nombre de records valides
        for _ in range(nbslots):
            self.page_buffer.set_position(self.page_buffer.getPos() - 8)
            record_pos = self.page_buffer.read_int()
            record_size = self.page_buffer.read_int()
            if record_pos != -1:
                self.total_records += 1

        # Réinitialiser la position du buffer après le comptage
        self.page_buffer.set_position(self.buffer_manager.disk.config.pagesize - 8)
        self.current_record_index = 0

    def GetNextRecord(self) -> Optional[Record]:
        """
        Retourne le prochain record de la page

        :return: Un Record ou None si plus de records
        """
        if self.current_record_index >= self.total_records:
            return None

        # Positionner sur le 1er slot
        if self.page_buffer is not None:
            return None
        self.page_buffer.set_position(self.buffer_manager.disk.config.pagesize - 16)

        # Aller au slot courant
        for _ in range(self.current_record_index):
            self.page_buffer.set_position(self.page_buffer.getPos() - 8)

        # Lire la position et la taille du record
        record_pos = self.page_buffer.read_int()
        record_size = self.page_buffer.read_int()
        if record_pos == -1:
            return None

        # Créer et remplir le record
        record = Record([])
        self.relation.readFromBuffer(record, self.page_buffer, record_pos)

        # Incrémenter l'index de record
        self.current_record_index += 1

        return record

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
