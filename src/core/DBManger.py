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

    #Supprimer une table de la BDD courante (Activ)
    def removeTableFromCurrentDatabase(self, table_name: str):
        if self.current_database:
            self.databases[self.current_database].removeTable(table_name)

    #Supprimer la BDD
    def removeDatabase(self, name: str):
        if name in self.databases:
            del self.databases[name]
            if self.current_database == name:
                self.current_database = None

    #Supprimer tous les tables la BDD courante (Activ)
    def removeTablesFromCurrentDatabase(self):
        if self.current_database:
            self.databases[self.current_database].tables.clear()

   #Supprimer tous les BDD
    def removeDatabases(self):
        self.databases.clear()
        self.current_database = None

    #Retourne la liste des BDD 
    def listDatabases(self) -> List[str]:
        return list(self.databases.keys())

    #Retourner ma liste des tables de la BDD courante (Active)
    def listTablesInCurrentDatabase(self) -> List[str]:
        if self.current_database:
            return self.databases[self.current_database].listTables()
        return []

    def saveState(self):
        # Écrire les enregistrements dans les buffers
        for db_name, db in self.databases.items():
            for table_name, table in db.tables.items():
                data_pages = table.getDataPages()
                for page_id in data_pages:
                    buffer = self.bufferManager.getPage(page_id)
                    records = table.getRecordsInDataPage(page_id)
                    pos = 0  # Position initiale dans le buffer
                    for record in records:
                        pos += table.writeRecordToBuffer(record, buffer, pos)
                    self.bufferManager.FreePage(page_id)

    def loadState(self):
        # Implémentez le chargement de l'état des bases de données
        pass

#tous les méthode remove doit etre revu !!!!