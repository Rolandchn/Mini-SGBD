from typing import List, Optional
from IRecordIterator import IRecordIterator
from Record import Record
from Relation import Relation
from PageId import PageId
from BufferManager import BufferManager
from DiskManager import DiskManager
from DataPageHoldRecordIterator import DataPageHoldRecordIterator
from Condition import Condition
from PageDirectoryIterator import PageDirectoryIterator

class JoinOperator(IRecordIterator):
    def __init__(self,
                 relation_r: Relation,
                 relation_s: Relation,
                 conditions: List[Condition],
                 buffer_manager: BufferManager,
                 disk_manager: DiskManager):
        """
        Initialise l'opérateur de jointure
        """
        self.relation_r = relation_r
        self.relation_s = relation_s
        self.conditions = conditions
        self.buffer_manager = buffer_manager
        self.disk_manager = disk_manager

        # Itérateurs pour parcourir les pages
        self.page_dir_r = PageDirectoryIterator(relation_r)

        # États de l'itération
        self.current_page_r = None
        self.current_record_r = None
        self.current_page_s = None
        self.current_record_s = None
        self.joined_records = []

    def _evaluate_conditions(self, record_r: Record, record_s: Record) -> bool:
        """
        Évalue les conditions de jointure entre deux records

        :param record_r: Record de la première relation
        :param record_s: Record de la deuxième relation
        :return: True si les conditions sont satisfaites
        """
        for condition in self.conditions:
            # Récupère les index des colonnes
            left_index = self.get_column_index(condition.left_term, self.relation_r)
            right_index = self.get_column_index(condition.right_term, self.relation_s)

            # Compare les valeurs selon l'opérateur
            left_value = record_r.values[left_index]
            right_value = record_s.values[right_index]

            if not self._compare_values(left_value, right_value, condition.operator):
                return False
        return True

    def _compare_values(self, left, right, operator: str) -> bool:
        """
        Compare deux valeurs selon l'opérateur

        :param left: Valeur de gauche
        :param right: Valeur de droite
        :param operator: Opérateur de comparaison
        :return: Résultat de la comparaison
        """
        if operator == '=':
            return left == right
        elif operator == '<':
            return left < right
        elif operator == '>':
            return left > right
        elif operator == '<=':
            return left <= right
        elif operator == '>=':
            return left >= right
        elif operator == '<>':
            return left != right
        return False

    def get_column_index(self, column_name: str, relation: Relation) -> int:
        """
        Récupère l'index d'une colonne dans une relation

        :param column_name: Nom de la colonne
        :param relation: Relation contenant la colonne
        :return: Index de la colonne
        """
        # Gère le cas où le nom de colonne contient un alias
        if '.' in column_name:
            column_name = column_name.split('.')[1]

        for i, col in enumerate(relation.columns):
            if col.name == column_name:
                return i

        raise ValueError(f"Colonne {column_name} non trouvée")

    def GetNextRecord(self) -> Optional[Record]:
        """
        Retourne le prochain record résultant de la jointure

        :return: Un Record de jointure ou None
        """
        # Si on a des records de jointure précédemment calculés, on les retourne
        if self.joined_records:
            return self.joined_records.pop(0)

        # Parcourt des pages de R
        while True:
            # Charge une nouvelle page de R si nécessaire
            if not self.current_page_r:
                page_id_r = self.page_dir_r.GetNextDataPageId()
                if not page_id_r:
                    # Plus de pages dans R
                    return None

                self.current_page_r = DataPageHoldRecordIterator(self.relation_r, page_id_r)

            # Récupère un record de R
            record_r = self.current_page_r.GetNextRecord()
            if record_r is None:
                # Fin de la page R, passe à la page suivante
                self.current_page_r.Close()
                self.current_page_r = None
                continue

            # Réinitialise le parcours de S si nécessaire
            if not self.current_page_s:
                page_dir_s = PageDirectoryIterator(self.relation_s)
                page_id_s = page_dir_s.GetNextDataPageId()
                if not page_id_s:
                    # Pas de pages dans S
                    return None

                self.current_page_s = DataPageHoldRecordIterator(self.relation_s, page_id_s)

            # Parcourt des pages de S
            while True:
                print("Boucle 2")
                record_s = self.current_page_s.GetNextRecord()
                if record_s is None:
                    # Fin de la page S, passe à la page suivante
                    self.current_page_s.Close()
                    print("if1")

                    page_id_s = page_dir_s.GetNextDataPageId()

                    if not page_id_s:
                        # Plus de pages dans S, recommence avec la prochaine page de R
                        break

                    self.current_page_s = DataPageHoldRecordIterator(self.relation_s, page_id_s)
                    continue

                # Évalue les conditions de jointure
                if self._evaluate_conditions(record_r, record_s):
                    # Combine les records de R et S
                    combined_values = record_r.values + record_s.values
                    self.joined_records.append(Record(combined_values))

            # Si des records de jointure ont été trouvés, retourne le premier
            if self.joined_records:
                return self.joined_records.pop(0)

    def Reset(self):
        """
        Réinitialise l'opérateur de jointure
        """
        # Réinitialise les itérateurs et états
        if self.current_page_r:
            self.current_page_r.Close()
        if self.current_page_s:
            self.current_page_s.Close()

        self.page_dir_r = PageDirectoryIterator(self.relation_r)
        self.current_page_r = None
        self.current_page_s = None
        self.joined_records = []

    def Close(self):
        """
        Libère toutes les ressources
        """
        if self.current_page_r:
            self.current_page_r.Close()
        if self.current_page_s:
            self.current_page_s.Close()

        self.page_dir_r.Close()
