from typing import Optional, List
from Database import Database 
from Relation import Relation

class DBManager:
    def __init__(self, db_config):
        self.databases = {}  # Dictionnaire pour stocker les bases de données
        self.current_database = None  # Variable pour stocker la base de données active
        self.db_config = db_config

    #Création d'une base de données
    def createDatabase(self, name: str):
        if name not in self.databases:
            self.databases[name] = Database(name)

    #Activer une base de données
    def setCurrentDatabase(self, name: str):
        if name in self.databases:
            self.current_database = name

    #Ajouter des tuples à la table active
    def addTableToCurrentDatabase(self, table: Relation):
        if self.current_database:
            self.databases[self.current_database].addTable(table)

    #Retourner une table dans la BDD en cour(Active)
    def getTableFromCurrentDatabase(self, table_name: str) -> Optional[Relation]:
        if self.current_database:
            return self.databases[self.current_database].getTable(table_name)

    def removeTableFromCurrentDatabase(self, table_name: str):
        if self.current_database:
            self.databases[self.current_database].removeTable(table_name)

    def removeDatabase(self, name: str):
        if name in self.databases:
            del self.databases[name]
            if self.current_database == name:
                self.current_database = None

    def removeTablesFromCurrentDatabase(self):
        if self.current_database:
            self.databases[self.current_database].tables.clear()

    def removeDatabases(self):
        self.databases.clear()
        self.current_database = None

    def listDatabases(self) -> List[str]:
        return list(self.databases.keys())

    def listTablesInCurrentDatabase(self) -> List[str]:
        if self.current_database:
            return self.databases[self.current_database].listTables()
        return []

    def saveState(self):
        # Implémentez la sauvegarde de l'état des bases de données
        pass

    def loadState(self):
        # Implémentez le chargement de l'état des bases de données
        pass
