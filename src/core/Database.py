from typing import Optional, List
from Relation import Relation
from pathlib import Path
import json
import os

class Database:
    def __init__(self, name: str):
        self.name = name
        self.tables = {}  # Dictionnaire pour stocker les tables
        self._unsaved_changes = False


    #Ajout de table dans le dictionnaire
    def addTable(self, table: Relation):
        if isinstance(table, Relation):
            self.tables[table.name] = table
        else:
            raise TypeError("L'objet ajouté doit être une instance de Relation")

    #Suppresson de table dans le dictionnaire
    def removeTable(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]
    
    #Trouver une table(Relation) selon le nom
    def getTable(self, table_name: str) -> Optional[Relation]:
        return self.tables.get(table_name)

    #Retourner tous les tables
    def listTables(self) -> List[str]:
        return list(self.tables.keys())
    
    def saveRelation(self, relation: 'Relation'):
        db_file_path = os.path.join(Path(__file__).parent,"..","..","storage","database",f"{self.name}.json")
        relation_data = {
            "name": relation.name,
            "nb_columns": relation.nb_column,
            "columns": [column.to_dict() for column in relation.columns],
            "headerPageId": {
                "fileIdx": relation.headerPageId.fileIdx,
                "pageIdx": relation.headerPageId.pageIdx
            }
        }

        if os.path.isfile(db_file_path):
            try:
                with open(db_file_path, "r", encoding="utf-8") as db_file:
                    data = json.load(db_file)
            except json.JSONDecodeError:
                data = []
        else:
            data = []

        updated = False
        for existing_relation in data:
            if existing_relation["name"] == relation.name:
                existing_relation.update(relation_data)
                updated = True
                break

        if not updated:
            data.append(relation_data)

        with open(db_file_path, "w", encoding="utf-8") as db_file:
            json.dump(data, db_file, indent=4, ensure_ascii=False)
    def markAsSaved(self):
        self._unsaved_changes = False
    def hasUnsavedChanges(self) -> bool:
        return self._unsaved_changes
    #Chargement de la base de données
    def loadDatabase(self):
        pass
    
    #Sauvegarde de la base de données
    def saveDatabase(self):
        pass
    
    
    

