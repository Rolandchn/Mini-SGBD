import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../core")))

import unittest
from unittest.mock import patch, MagicMock
from DBManager import DBManager
from Database import Database
from Relation import Relation
from BufferManager import BufferManager
import os
import json

class TestDBManager(unittest.TestCase):

    def setUp(self):
        self.db_config = MagicMock()
        self.db_manager = DBManager(self.db_config)

    def test_createDatabase(self):
        self.assertTrue(self.db_manager.createDatabase("test_db"))
        self.assertFalse(self.db_manager.createDatabase("test_db"))

    def test_setCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.assertTrue(self.db_manager.setCurrentDatabase("test_db"))
        self.assertFalse(self.db_manager.setCurrentDatabase("non_existent_db"))

    def test_addTableToCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table = MagicMock(spec=Relation)
        self.assertTrue(self.db_manager.addTableToCurrentDatabase(table))

    def test_getTableFromCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table)
        self.assertEqual(self.db_manager.getTableFromCurrentDatabase("test_table"), table)

    def test_removeTableFromCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table)
        self.assertTrue(self.db_manager.removeTableFromCurrentDatabase("test_table"))

    def test_removeDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.assertTrue(self.db_manager.removeDatabase("test_db"))
        self.assertFalse(self.db_manager.removeDatabase("non_existent_db"))

    def test_removeTablesFromCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table1 = MagicMock(spec=Relation)
        table2 = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table1)
        self.db_manager.addTableToCurrentDatabase(table2)
        self.assertTrue(self.db_manager.removeTablesFromCurrentDatabase())

    def test_removeDatabases(self):
        self.db_manager.createDatabase("test_db1")
        self.db_manager.createDatabase("test_db2")
        self.assertTrue(self.db_manager.removeDatabases())

    def test_listDatabases(self):
        self.db_manager.createDatabase("test_db1")
        self.db_manager.createDatabase("test_db2")
        self.assertEqual(self.db_manager.listDatabases(), ["test_db1", "test_db2"])

    def test_listTablesInCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table1 = MagicMock(spec=Relation)
        table2 = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table1)
        self.db_manager.addTableToCurrentDatabase(table2)
        self.assertEqual(self.db_manager.listTablesInCurrentDatabase(), ["test_table1", "test_table2"])

    def test_listColumnInfoInCurrentDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table1 = MagicMock(spec=Relation)
        table2 = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table1)
        self.db_manager.addTableToCurrentDatabase(table2)
        self.assertEqual(self.db_manager.listColumnInfoInCurrentDatabase(), [table1.columns, table2.columns])

    def test_deleteDatabase(self):
        self.db_manager.createDatabase("test_db")
        self.assertTrue(self.db_manager.deleteDatabase("test_db"))
        self.assertFalse(self.db_manager.deleteDatabase("non_existent_db"))

    @patch('builtins.open', new_callable=MagicMock)
    def test_saveState(self, mock_open):
        self.db_manager.createDatabase("test_db")
        self.db_manager.setCurrentDatabase("test_db")
        table = MagicMock(spec=Relation)
        self.db_manager.addTableToCurrentDatabase(table)

        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps([{"name": "test_db", "tables": [{"name": "test_table"}]}])

        self.db_manager.saveState()
        mock_open.assert_called_once_with(os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json"), 'r', encoding="utf-8")

    @patch('builtins.open', new_callable=MagicMock)
    def test_loadState(self, mock_open):
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps([{"name": "test_db", "tables": [{"name": "test_table"}]}])

        self.db_manager.loadState()
        mock_open.assert_called_once_with(os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json"), 'r', encoding="utf-8")

    @patch('builtins.open', new_callable=MagicMock)
    def test_removeDatabaseFromSaveFile(self, mock_open):
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps([{"name": "test_db"}])

        self.db_manager.removeDatabaseFromSaveFile("test_db")
        mock_open.assert_called_once_with(os.path.join(os.path.dirname(__file__), "..", "..", "storage", "db.save.json"), 'r', encoding="utf-8")

    @patch('os.remove')
    def test_deleteDatabaseFile(self, mock_remove):
        self.db_manager.deleteDatabaseFile("test_db")
        mock_remove.assert_called_once_with(os.path.join(os.path.dirname(__file__), "..", "..", "storage", "database", "test_db.json"))

if __name__ == '__main__':
    unittest.main()
