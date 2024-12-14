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
        self.bufferManager = BufferManager.setup(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json"))

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
            return True
        return False
    #Ajouter des tuples à la table active
    def addTableToCurrentDatabase(self, table: Relation):
        if self.current_database:
            print("table added")
            self.databases[self.current_database].addTable(table)
            return True
        return False
    #Retourner une table dans la BDD en cour(Active)
    def getTableFromCurrentDatabase(self, table_name: str) -> Optional[Relation]:
        if self.current_database:
            return self.databases[self.current_database].getTable(table_name)
        else:
            return None
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
                return True
            return False
        return False

    #Supprimer tous les tables la BDD courante (Activ)
    def removeTablesFromCurrentDatabase(self):
        if self.current_database:
            self.databases[self.current_database].tables.clear()
            return True
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

    def deleteDatabase(self, name: str):
        """
        Supprime une base de données du dictionnaire.
        """
        if name in self.databases:
            del self.databases[name]
            print(f"Database {name} removed from dictionary")
        else:
            raise ValueError("La base de données n'existe pas")

    def saveState(self):
        """
        Synchronise l'état des bases de données avec le fichier principal db.save.json et leurs fichiers respectifs.
        """
        file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json")

        if os.path.exists(file_path):
            with open(file_path, 'r', encoding="utf-8") as file:
                try:
                    saved_data = json.load(file)
                except json.JSONDecodeError:
                    saved_data = []
        else:
            saved_data = []

        current_database_names = set(self.databases.keys())

        for name, database in self.databases.items():
            filename = f"{name}.json"
            db_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "database", filename)

            if not os.path.exists(db_file_path):
                with open(db_file_path, 'w', encoding="utf-8") as db_file:
                    json.dump([], db_file)

            # Charger les données actuelles du fichier JSON de la base de données
            with open(db_file_path, 'r', encoding="utf-8") as db_file:
                try:
                    db_data = json.load(db_file)
                except json.JSONDecodeError:
                    db_data = []

            # Mettre à jour les relations dans le fichier JSON
            updated_db_data = []
            for table_name, table in database.tables.items():
                table_data = database.saveRelation(table)
                updated_db_data.append(table_data)

            # Supprimer les relations qui ne sont plus présentes
            for relation in db_data:
                if relation['name'] not in database.tables:
                    print(f"Suppression de la relation {relation['name']} de la base de données {name}")

            # Écrire les données mises à jour dans le fichier JSON
            with open(db_file_path, 'w', encoding="utf-8") as db_file:
                json.dump(updated_db_data, db_file, indent=4, ensure_ascii=False)

            database.markAsSaved()

        # Ajouter les nouvelles bases de données à saved_data
        for db_name in current_database_names:
            if db_name not in saved_data:
                saved_data.append(db_name)
                print(f"Database {db_name} added to db.save.json")

        # Supprimer les bases de données qui ne sont plus dans le dictionnaire
        for db_name in saved_data:
            if db_name not in current_database_names:
                self.removeDatabaseFromSaveFile(db_name)
                self.deleteDatabaseFile(db_name)

        saved_data = [db_name for db_name in saved_data if db_name in current_database_names]

        with open(file_path, 'w', encoding="utf-8") as file:
            json.dump(saved_data, file, indent=4, ensure_ascii=False)

    def removeDatabaseFromSaveFile(self, name: str):
        """
        Supprime une base de données du fichier db.save.json.
        """
        file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json")

        if os.path.exists(file_path):
            with open(file_path, 'r', encoding="utf-8") as file:
                try:
                    saved_data = json.load(file)
                except json.JSONDecodeError:
                    saved_data = []
        else:
            saved_data = []

        if name in saved_data:
            saved_data.remove(name)
            print(f"Database {name} removed from db.save.json")

        with open(file_path, 'w', encoding="utf-8") as file:
            json.dump(saved_data, file, indent=4, ensure_ascii=False)

    def deleteDatabaseFile(self, name: str):
        """
        Supprime le fichier JSON correspondant à la base de données.
        """
        db_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "database", f"{name}.json")

        if os.path.exists(db_file_path):
            os.remove(db_file_path)
            print(f"Fichier {db_file_path} supprimé")
        else:
            print(f"Le fichier {db_file_path} n'existe pas")
            
            
    def loadState(self):
        """
        Charge l'état des bases de données à partir du fichier principal db.save.json et leurs fichiers respectifs.
        """
        main_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json")

        # Charger le fichier principal db.save.json
        if os.path.exists(main_file_path):
            with open(main_file_path, 'r', encoding="utf-8") as main_file:
                try:
                    saved_data = json.load(main_file)
                except json.JSONDecodeError:
                    print(f"Erreur de décodage JSON pour le fichier {main_file_path}")
                    saved_data = []
        else:
            print(f"Le fichier {main_file_path} n'existe pas")
            saved_data = []

        # Charger chaque base de données
        for db_name in saved_data:
            db_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "database", f"{db_name}.json")

            if os.path.exists(db_file_path):
                with open(db_file_path, 'r', encoding="utf-8") as db_file:
                    try:
                        db_data = json.load(db_file)
                    except json.JSONDecodeError:
                        print(f"Erreur de décodage JSON pour le fichier {db_file_path}")
                        db_data = []

                # Créer la base de données si elle n'existe pas déjà
                if db_name not in self.databases:
                    self.createDatabase(db_name)

                database = self.databases[db_name]

                # Charger chaque table dans la base de données
                for table_data in db_data:
                    table_name = table_data['name']
                    table = Relation.loadRelation(table_name, self.bufferManager.disk, self.bufferManager, db_name)
                    database.tables[table_name] = table

                database.markAsLoaded()
            else:
                print(f"Le fichier {db_file_path} n'existe pas")

        # Supprimer les bases de données qui ne sont pas dans l'état sauvegardé
        current_database_names = set(self.databases.keys())
        saved_database_names = set(saved_data)
        for db_name in current_database_names - saved_database_names:
            del self.databases[db_name]
            
#tous les méthode remove doit etre revu !!!!
