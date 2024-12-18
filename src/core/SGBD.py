import os
import traceback

from typing import List

import Column

from DiskManager import DiskManager
from BufferManager import BufferManager
from DBconfig import DBconfig
from DBManager import DBManager

from Column import ColumnInfo
from Relation import Relation
from Record import Record
from Condition import Condition

from ProjectOperator import ProjectOperator
from RecordPrinter import RecordPrinter
from SelectOperator import SelectOperator
from RelationScanner import RelationScanner

import traceback
import csv


class SGBD:
    def __init__(self, db_config: DBconfig):
        self.db_config = db_config
        self.disk_manager = DiskManager(db_config)
        self.buffer_manager = BufferManager(db_config, self.disk_manager)
        self.db_manager = DBManager(db_config)

        # Charger l'état des composants
        self.disk_manager.LoadState()
        self.db_manager.loadState()


    def run(self) -> None:
        while True:
            command = input("Enter command: ")

            if command.upper() == "QUIT":
                self.processQuitCommand()
                break

            else:
                self.processCommand(command)


    def processCommand(self, command: str) -> None:
        # Analyser et traiter la commande
        try:
            parts = command.split()

            if not parts:
                return

            cmd = parts[0].upper()
            arg1 = parts[1].upper()

            if cmd == "CREATE":
                if arg1 == "DATABASE":
                    self.processCreateDatabaseCommand(parts[2])

                elif arg1 == "TABLE":
                    self.processCreateTableCommand(parts[2:])

            elif cmd == "SET":
                if arg1 == "DATABASE":
                    self.processSetDatabaseCommand(parts[2])

            elif cmd == "DROP":
                if arg1 == "DATABASE":
                    self.processDropDatabaseCommand(parts[2])

                elif arg1 == "TABLE":
                    self.processDropTableCommand(parts[2])

                elif arg1 == "TABLES":
                    self.processDropTablesCommand()

                elif arg1 == "DATABASES":
                    self.processDropDatabasesCommand()

            elif cmd == "LIST":
                if arg1 == "DATABASES":
                    self.processListDatabasesCommand()

                elif arg1 == "TABLES":
                    self.processListTablesCommand()

            elif cmd == "INSERT":        
                self.processInsertCommand(command)

            elif cmd == "BULKINSERT":
                self.processBulkInsertCommand(parts[1:])

            elif cmd == "SELECT":
                self.processSelectCommand(parts[1:])

            else:
                print("Unknown command")

        except IndexError as I:
            print("Arguments insufficient for command.", I)
            traceback.print_exc()

        except Exception as e:
            traceback.print_exc() 


    def processQuitCommand(self) -> None:
        # Sauvegarder l'état avant de quitter
        self.buffer_manager.FlushBuffers()
        self.disk_manager.SaveState()
        self.db_manager.saveState()

        print("SGBD stopped. State saved.")


    def processCreateDatabaseCommand(self, db_name: str) -> None:
        self.db_manager.createDatabase(db_name)

        print(f"Database {db_name} created.")


    def processSetDatabaseCommand(self, db_name: str) -> None:
        if self.db_manager.setCurrentDatabase(db_name):
            print(f"Database set to {db_name}.")

        else:
            print(f"Database {db_name} does not exist.")


    def processCreateTableCommand(self, parts: list[str]) -> None:
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
                print(f"Failed to create table {table_name} in the current database, Check if Database is set.")
       
        else:
            print("Invalid column definitions.")


    def processDropDatabaseCommand(self, db_name: str) -> None:
        if self.db_manager.removeDatabase(db_name):
            print(f"Database {db_name} dropped.")

        else:
            print(f"Database {db_name} does not exist.")


    def processDropTableCommand(self, table_name: str) -> None:
        if self.db_manager.removeTableFromCurrentDatabase(table_name):
            print(f"Table {table_name} dropped from the current database.")

        else:
            print(f"Table {table_name} does not exist in the current database.")


    def processDropTablesCommand(self) -> None:
        if self.db_manager.removeTablesFromCurrentDatabase():
            print("All tables dropped from the current database.")

        else:
            print("No tables to drop in the current database.")


    def processDropDatabasesCommand(self) -> None:
        if self.db_manager.removeDatabases():
            print("All databases dropped.")

        else:
            print("No databases to drop.")


    def processListDatabasesCommand(self) -> None:
        databases = self.db_manager.listDatabases()

        if databases:
            print("Databases:")

            for db in databases:
                print(db)

        else:
            print("No databases found.")


    def processListTablesCommand(self) -> None:
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
            
            # Remove quotes from the beginning and end of the string
            # Handle different quote types: ", ', ʺ
            if (value_part.startswith('"') and value_part.endswith('"')) or \
            (value_part.startswith("'") and value_part.endswith("'")) or \
            (value_part.startswith("ʺ") and value_part.endswith("ʺ")):
                value_part = value_part[1:-1]
            
            # Convert to appropriate type
            if value_part.isdigit():
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

    def processBulkInsertCommand(self, parts: list[str]):

        if parts[0].upper() != "INTO":
            print("Invalid command.")
            return

        table_name = parts[1]
        file_name = parts[2]

        file_name = os.path.join(os.path.dirname(__file__), file_name)

        if not os.path.exists(file_name):
            print(f"File {file_name} not found. Please check the path.")
            return

        # Récupérer la table de la base de données courante
        table = self.db_manager.getTableFromCurrentDatabase(table_name)
        if table is None:
            print(f"Table {table_name} does not exist.")
            return

        # Lire le fichier CSV
        try:
            with open(file_name, 'r') as csvfile:
                reader = csv.reader(csvfile)

                # Parcourir chaque ligne du fichier
                for row in reader:
                    if len(row) != table.nb_column:
                        print(f"Invalid CSV line: {row}. Number of values does not match the number of columns in table {table_name}.")
                        continue

                    # Construire la commande INSERT
                    values = ", ".join([f"'{value.strip()}'" for value in row])
                    insert_command = f"INSERT INTO {table_name} VALUES ({values})"


                    self.processInsertCommand(insert_command)
                    

            print(f"All records from {file_name} have been processed and inserted into {table_name}.")
        except Exception as e:
            print(f"An error occurred while processing the file: {e}")

    def processSelectCommand(self, parts: list[str]) -> None:
        command = " ".join(parts)

        if len(parts) < 4 or parts[1].upper() != "FROM":
            print("Invalid SELECT command.")
            return

        # Extraire les colonnes et le nom de la table
        columns_part = parts[0]
        table_part = parts[2]
        print(table_part)

        table_name = table_part
        table_alias = parts[3]

        conditions = self.parseConditions(command)

        if self.db_manager.current_database:
            if table := self.db_manager.getTableFromCurrentDatabase(table_name):
                relation_scanner = RelationScanner(table)
                select_operator = SelectOperator(relation_scanner, 
                                                                [Condition(c.left_term, c.operator, c.right_term, table_alias) 
                                                                for c in conditions], 
                                                                table)
                # Remplacer les alias de colonne par les noms de colonne réels
                if table_alias:
                    columns = [f"{table_alias}.{col}" if '.' not in col else col for col in columns_part.split(",")]
               
                else:
                    columns = columns_part.split(",")

                if columns[0] == '*' or columns[0] == table_alias+'.*':
                    columns = [f"{table_alias}.{col.name}" if table_alias else col.name for col in table.columns]

                project_operator = ProjectOperator(select_operator, columns, table, table_alias)
                printer = RecordPrinter(project_operator)
                printer.print_records()
            
            else:
                print(f"Table {table_name} does not exist.")
       
        else:
            print("No current database set.")


    @staticmethod
    def parseConditions(command: str) -> List[Condition]:
        conditions = []
        
        if "WHERE" in command:
            condition_str = command.split("WHERE")[1]
            condition_parts = condition_str.split("AND")
          
            for condition_part in condition_parts:
                conditions.append(Condition.from_string(condition_part))
       
        return conditions
    

if __name__ == "__main__":
    sgbd = SGBD(DBconfig.LoadDBConfig(os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")))
    sgbd.run()
        

#TODO ne devrait pas afficher un log de succès tant que ça a pas marché
#TODO desalouer les pages des bases et tables supprimées
#TODO revoir la condition de create table
#TODO revoir la condition de insert (on peut pas insérer des record avec chat de taille 1 (taille de 2) mais on peut inserer plus de 2 char)
#REMARQUE : on peut pas mettre d'accents dans les records