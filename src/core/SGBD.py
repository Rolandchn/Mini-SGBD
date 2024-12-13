from typing import Optional
from DBManger import DBManager
from DiskManager import DiskManager
from BufferManager import BufferManager
from DBconfig import DBconfig
from Column import ColumnInfo
import Column
from Relation import Relation
import os
from pathlib import Path


class SGBD:
    def __init__(self, db_config:DBconfig):
        self.db_config = db_config
        self.disk_manager = DiskManager(db_config)
        self.buffer_manager = BufferManager(db_config, self.disk_manager)
        self.db_manager = DBManager(db_config)

        # Charger l'état des composants
        self.disk_manager.LoadState()
        self.db_manager.loadState()

    def run(self):
        while True:
            command = input("Enter command: ")
            if command.upper() == "QUIT":
                self.processQuitCommand()
                break
            else:
                self.processCommand(command)

    def processCommand(self, command: str):
        # Analyser et traiter la commande
        parts = command.split()
        if not parts:
            return

        cmd = parts[0].upper()
        if cmd == "CREATE":
            if parts[1].upper() == "DATABASE":
                self.processCreateDatabaseCommand(parts[2])
            elif parts[1].upper() == "TABLE":
                self.processCreateTableCommand(parts[2:])
        elif cmd == "SET":
            if parts[1].upper() == "DATABASE":
                self.processSetDatabaseCommand(parts[2])
        elif cmd == "DROP":
            if parts[1].upper() == "DATABASE":
                self.processDropDatabaseCommand(parts[2])
            elif parts[1].upper() == "TABLE":
                self.processDropTableCommand(parts[2])
            elif parts[1].upper() == "TABLES":
                self.processDropTablesCommand()
            elif parts[1].upper() == "DATABASES":
                self.processDropDatabasesCommand()
        elif cmd == "LIST":
            if parts[1].upper() == "DATABASES":
                self.processListDatabasesCommand()
            elif parts[1].upper() == "TABLES":
                self.processListTablesCommand()
        elif cmd == "INSERT":        
            self.processInsertCommand(parts[1:])
        elif cmd == "BULKINSERT":
            self.processBulkInsertCommand(parts[1:])
        elif cmd == "SELECT":
            self.processSelectCommand(parts[1:])
        else:
            print("Unknown command")

    def processQuitCommand(self):
        # Sauvegarder l'état avant de quitter
        self.buffer_manager.FlushBuffers()
        self.disk_manager.SaveState()
        self.db_manager.saveState()
        print("SGBD stopped. State saved.")

    def processCreateDatabaseCommand(self, db_name: str):
        self.db_manager.createDatabase(db_name)
        print(f"Database {db_name} created.")

    def processSetDatabaseCommand(self, db_name: str):
        self.db_manager.setCurrentDatabase(db_name)
        print(f"Database set to {db_name}.")

    def processCreateTableCommand(self, parts: list[str]):
        '''if len(parts) < 4:
            print("Invalid CREATE TABLE command.")
            return'''

        table_name = parts[0]
        columns = self.parseColumns(parts[1])
        table = Relation(table_name, len(columns), columns, self.disk_manager, self.buffer_manager)
        self.db_manager.addTableToCurrentDatabase(table)
        print(f"Table {table_name} created in the current database.")

    def processDropDatabaseCommand(self, db_name: str):
        self.db_manager.removeDatabase(db_name)
        print(f"Database {db_name} dropped.")

    def processDropTableCommand(self, table_name: str):
        self.db_manager.removeTableFromCurrentDatabase(table_name)
        print(f"Table {table_name} dropped from the current database.")

    def processDropTablesCommand(self):
        self.db_manager.removeTablesFromCurrentDatabase()
        print("All tables dropped from the current database.")

    def processDropDatabasesCommand(self):
        self.db_manager.removeDatabases()
        print("All databases dropped.")

    def processListDatabasesCommand(self):
        databases = self.db_manager.listDatabases()
        print("Databases:")
        for db in databases:
            print(db)

    def processListTablesCommand(self):
        tables = self.db_manager.listTablesInCurrentDatabase()
        print("Tables in the current database:")
        for table in tables:
            print(table)

    @staticmethod
    def parseColumns(columns_str: str) -> list[ColumnInfo]:
        columns = []
        column_parts = columns_str[1:-1].split(",")
        for column_part in column_parts:
            name, type_str = column_part.split(":")
            if type_str == "INT":
                columns.append(ColumnInfo(name, Column.Int()))
            elif type_str == "REAL":
                columns.append(ColumnInfo(name, Column.Float()))
            elif type_str.startswith("CHAR("):
                size = int(type_str[5:-1])
                columns.append(ColumnInfo(name, Column.Char(size)))
            elif type_str.startswith("VARCHAR("):
                size = int(type_str[8:-1])
                columns.append(ColumnInfo(name, Column.VarChar(size)))
        return columns
    
    
    @staticmethod
    def parseValues(columns_str: str) -> list[ColumnInfo]:
        columns = []
        column_parts = columns_str[1:-1].split(",")
        for column_part in column_parts:
                columns.append(column_part)
                
        return columns
    
    
    #TP 7 START GO GO GO 
    def processInsertCommand(self,reste: list[str]):
        # Récupérer la table de la base de données courante
            if reste[0].upper() == "INTO" and reste[2].upper() == "VALUES":
                table = self.db_manager.getTableFromCurrentDatabase(reste[1])
                if table is None:
                    print(f"Table {reste[1]} does not exist.")
                    return
                # Vérifier que le nombre de valeurs correspond au nombre de colonnes
                values = self.parseValues(reste[3])
                if len(values) != table.nb_column:
                    print(f"Number of values does not match the number of columns in table {reste[1]}.")
                    return
                # Convertir les valeurs en types appropriés
                typed_values = []
                for i, value in enumerate(values):
                    column_type = table.columns[i].type
                    if column_type == Column.Int():
                        typed_values.append(int(value))
                    elif column_type == Column.Float():
                        typed_values.append(float(value))
                    elif column_type == Column.Char(column_type.size) and len(value) == column_type.size:
                        typed_values.append(value)
                    elif column_type == Column.VarChar(column_type.size) and len(value) <= column_type.size:
                        typed_values.append(value)
                    else:
                        print("Invalid column type.")
                        return
                print(typed_values)
                # Insérer le tuple dans la table
                table.InsertRecord(typed_values)
                print(f"Record inserted into table {reste[1]}.")
            else:
                print("Invalid INSERT command.")



    def processSelectCommand(self, command: str):
        parts = command.split()
        if len(parts) < 4 or parts[1].upper() != "FROM":
            print("Invalid SELECT command.")
            return

        columns = parts[1].split(",")
        table_name = parts[3]
        conditions = self.parseConditions(command)

        if self.db_manager.current_database:
            table = self.db_manager.current_database.tables.get(table_name)
            if table:
                self.selectRecords(table, columns, conditions)
            else:
                print(f"Table {table_name} does not exist.")
        else:
            print("No current database set.")

    def selectRecords(self, table: Relation, columns: list[str], conditions: list):
        all_records = table.GetAllRecords()
        filtered_records = []

        for record in all_records:
            if all(condition.evaluate(record, table.columns) for condition in conditions):
                filtered_records.append(record)

        for record in filtered_records:
            print("; ".join(record.values) + ".")
        print(f"Total records={len(filtered_records)}")
        
if __name__ == "__main__":
    sgbd = SGBD(DBconfig.LoadDBConfig(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")))
    sgbd.run()
    

#TODO ne devrait pas afficher un log de succès tant que ça a pas marché
#TODO desalouer les pages des bases et tables supprimées
#TODO revoir la condition de create table

#REMARQUE : on peut pas mettre d'accents dans les records