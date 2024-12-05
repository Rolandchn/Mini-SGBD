from typing import Optional, List
from Relation import Relation

class Database:
    def __init__(self, name: str):
        self.name = name
        self.tables = {}  # Dictionnaire pour stocker les tables

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


