from typing import Optional, List
from Database import Database 
from Relation import Relation
import os
from pathlib import Path
from DBconfig import DBconfig   
from BufferManager import BufferManager
from Column import ColumnInfo
import Column
import json
class DBManager:
    def __init__(self, db_config):
        self.databases = {}  # Dictionnaire pour stocker les bases de données
        self.current_database = None  # Variable pour stocker la base de données active
        self.db_config = db_config

    #Création d'une base de données
    def createDatabase(self, name: str):
        if name not in self.databases:
            print("database created")
            self.databases[name] = Database(name)
        else:
            raise ValueError("La base de données existe déjà")
        
            
    #Activer une base de données
    def setCurrentDatabase(self, name: str):
        if name in self.databases:
            self.current_database = name

    #Ajouter des tuples à la table active
    def addTableToCurrentDatabase(self, table: Relation):
        if self.current_database:
            print("table added")
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
        # Chemin vers le fichier JSON principal
        file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json")

        # Charger les bases de données existantes à partir du fichier s'il existe
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                try:
                    saved_data = json.load(file)
                except json.JSONDecodeError:
                    saved_data = []
        else:
            saved_data = []

        # Identifier les bases de données nécessitant une sauvegarde (nouvelles ou modifiées)
        saved_names = set(saved_data)
        to_save_databases = []

        for name, database in self.databases.items():
            # Vérifier si le fichier de la base de données existe déjà
            filename = f"{name}.json"
            db_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "database", filename)
            db_exists = os.path.exists(db_file_path)

            if not db_exists:
                with open(db_file_path, 'w') as file:
                    json.dump([], file)
            # Vérifier si la base de données est nouvelle, modifiée, ou si son fichier n'existe pas
            for table_name, table in database.tables.items():
                database.saveRelation(table)

                # Marquer la base de données comme sauvegardée
            database.markAsSaved()

        # Mettre à jour le fichier JSON principal (db.save.json)
        if to_save_databases:
            saved_data.extend(name for name in to_save_databases if name not in saved_names)
            with open(file_path, 'w') as file:
                json.dump(saved_data, file, indent=4)

#tous les méthode remove doit etre revu !!!!
if __name__ == "__main__":
    liste = [Column.ColumnInfo("test1", Column.VarChar(5)), Column.ColumnInfo("test2", Column.Int())]
    bufferManager = BufferManager.setup(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))
    bufferManager.disk.LoadState()
    db_manager = DBManager(DBconfig.LoadDBConfig(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")))
    db_manager.createDatabase("db1")
    db_manager.createDatabase("db2")
    db_manager.setCurrentDatabase("db1")
    db_manager.createDatabase("db3")
    db_manager.removeDatabase("db1")
    db_manager.removeDatabase("db3")
    print(db_manager.listDatabases())
    db_manager.setCurrentDatabase("db2")
    print(db_manager.listDatabases())
    script_dir = Path(__file__).parent
    db_file_path = script_dir / "../../storage/database/test1.json"
    db_manager.addTableToCurrentDatabase(Relation.loadRelation("test6",bufferManager.disk, bufferManager,"db2"))
    db_manager.addTableToCurrentDatabase(Relation.loadRelation("test4",bufferManager.disk, bufferManager,"db2"))
    print(db_manager.listTablesInCurrentDatabase())
    db_manager.listTablesInCurrentDatabase()
    print(db_manager.listTablesInCurrentDatabase())
    db_manager.saveState()
