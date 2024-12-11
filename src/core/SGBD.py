from typing import Optional
from DBManger import DBManager
from DiskManager import DiskManager
from BufferManager import BufferManager
from DBconfig import DBconfig
from Column import ColumnInfo
import Column


class SGBD:
    def __init__(self, db_config:DBconfig):
        self.db_config = db_config
        self.disk_manager = DiskManager(db_config)
        self.buffer_manager = BufferManager(db_config, self.disk_manager)
        self.db_manager = DBManager(db_config, self.buffer_manager)

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

    def processCreateTableCommand(self, parts: List[str]):
        if len(parts) < 4:
            print("Invalid CREATE TABLE command.")
            return

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