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
from Record import Record

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
            self.processInsertCommand(command)
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
        if self.db_manager.setCurrentDatabase(db_name):
            print(f"Database set to {db_name}.")
        else:
            print(f"Database {db_name} does not exist.")


    def processCreateTableCommand(self, parts: list[str]):
        if len(parts) < 2:
            print("Invalid CREATE TABLE command.")
            return

        table_name = parts[0]
        columns = self.parseColumns(parts[1])
        if columns:
            table = Relation(table_name, len(columns), columns, self.disk_manager, self.buffer_manager)
            if self.db_manager.addTableToCurrentDatabase(table):
                print(f"Table {table_name} created in the current database.")
            else:
                print(f"Failed to create table {table_name} in the current database.")
        else:
            print("Invalid column definitions.")


    def processDropDatabaseCommand(self, db_name: str):
        if self.db_manager.removeDatabase(db_name):
            print(f"Database {db_name} dropped.")
        else:
            print(f"Database {db_name} does not exist.")

    def processDropTableCommand(self, table_name: str):
        if self.db_manager.removeTableFromCurrentDatabase(table_name):
            print(f"Table {table_name} dropped from the current database.")
        else:
            print(f"Table {table_name} does not exist in the current database.")

    def processDropTablesCommand(self):
        if self.db_manager.removeTablesFromCurrentDatabase():
            print("All tables dropped from the current database.")
        else:
            print("No tables to drop in the current database.")

    def processDropDatabasesCommand(self):
        if self.db_manager.removeDatabases():
            print("All databases dropped.")
        else:
            print("No databases to drop.")

    def processListDatabasesCommand(self):
        databases = self.db_manager.listDatabases()
        if databases:
            print("Databases:")
            for db in databases:
                print(db)
        else:
            print("No databases found.")

    def processListTablesCommand(self):
        tables = self.db_manager.listTablesInCurrentDatabase()
        if tables:
            print("Tables in the current database:")
            for table in tables:
                print(table)
        else:
            print("No tables found in the current database.")

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
    
    
    def parseValues(self, values_str: str) -> list:
        values = []
        value_parts = values_str.split(",")
        for value_part in value_parts:
            value_part = value_part.strip()
            if value_part.startswith('"') and value_part.endswith('"'):
                values.append(value_part[1:-1])
            elif value_part.isdigit():
                values.append(int(value_part))
            elif value_part.replace('.', '', 1).isdigit():
                values.append(float(value_part))
            else:
                values.append(value_part)
        return values

    
    #TP 7 START GO GO GO
    def processInsertCommand(self, command: str):
        parts = command.split()
        if len(parts) < 5 or parts[1].upper() != "INTO" or parts[3].upper() != "VALUES":
            print("Invalid INSERT command.")
            return

        table_name = parts[2]
        values_str = parts[4][1:-1]
        typed_values = self.parseValues(values_str)

        if self.db_manager.current_database:
            table = self.db_manager.getTableFromCurrentDatabase(table_name)
            if table:
                record = Record(typed_values)
                rid = table.InsertRecord(record)
                if rid is not None:
                    print(f"Record inserted with RID: {rid}")
                else:
                    print("Failed to insert record.")
            else:
                print(f"Table {table_name} does not exist.")
        else:
            print("No current database set.")
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