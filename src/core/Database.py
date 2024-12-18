from typing import Optional, List
from Relation import Relation
from pathlib import Path
import json
import os

class Database:
    def __init__(self, name: str):
        self.name = name
        self.tables: dict[str, Relation] = {}  # Dictionnaire pour stocker les tables
        self._unsaved_changes = False


    #Ajout de table dans le dictionnaire
    def addTable(self, table: Relation):
        if isinstance(table, Relation):
            self.tables[table.name] = table
        else:
            raise TypeError("L'objet ajouté doit être une instance de Relation")


    #Suppresson de table dans le dictionnaire
    def removeTable(self, table_name: str) -> bool:
        if table_name in self.tables:
            del self.tables[table_name]

            return True
        return False


    #Trouver une table(Relation) selon le nom
    def getTable(self, table_name: str) -> Optional[Relation]:
        return self.tables.get(table_name)


    #Retourner tous les tables
    def listTables(self) -> List[str]:
        return list(self.tables.keys())
    
    
    def saveRelation(self, relation: 'Relation'):
        """
        Retourne les données de la relation sous forme de dictionnaire.
        """
        relation_data = {
            "name": relation.name,
            "nb_columns": relation.nb_column,
            "columns": [column.to_dict() for column in relation.columns],
            "headerPageId": {
                "fileIdx": relation.headerPageId.fileIdx,
                "pageIdx": relation.headerPageId.pageIdx
            }
        }
        return relation_data

    def markAsSaved(self):
        self._unsaved_changes = False

    def hasUnsavedChanges(self) -> bool:
        return self._unsaved_changes

    def markAsLoaded(self):
        """
        Marque la base de données comme étant chargée.
        """
        self.loaded = True
        print(f"Database {self.name} loaded.")