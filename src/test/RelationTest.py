import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../core")))

import unittest
from unittest.mock import patch, MagicMock
from Relation import Relation
from DiskManager import DiskManager
from BufferManager import BufferManager
from PageId import PageId
from Buffer import Buffer
from Record import Record
from RecordId import RecordId
from Column import ColumnInfo, Int, Float, Char, VarChar
from DBconfig import DBconfig
import os
import json


class TestRelation(unittest.TestCase):

    def setUp(self):
        config_file = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
        self.config = DBconfig.LoadDBConfig(config_file)
        self.disk = DiskManager(self.config)
        self.buffer_manager = BufferManager(self.config, self.disk)
        self.columns = [ColumnInfo(name="col1", type=Int(4)), ColumnInfo(name="col2", type=VarChar(10))]
        self.relation = Relation(name="test_relation", nb_column=2, columns=self.columns, disk=self.disk, bufferManager=self.buffer_manager)


    @patch('os.path.exists')  # Mocking os.path.exists globally
    @patch('builtins.open', new_callable=MagicMock)  # Mocking the open function

    @patch('os.path.exists')  # Mocking os.path.exists globally
    @patch('builtins.open', new_callable=MagicMock)  # Mocking the open function
    def test_loadRelation(self, mock_open, mock_path_exists):
        # Simuler l'existence des fichiers db.save.json et de la relation
        mock_path_exists.side_effect = lambda x: x in [
            "../../storage/db.save.json",  # Simulé
            "../../storage/database/test_relation.json"  # Simulé
        ]


        # Contenu simulé de la relation dans ../../storage/database/test_relation.json
        simulated_relation_json = json.dumps([{
            "name": "test_relation",
            "nb_columns": 2,
            "columns": [
                {"name": "col1", "type": {"type": "Int", "size": 4}},
                {"name": "col2", "type": {"type": "VarChar", "size": 10}}
            ],
            "headerPageId": {"fileIdx": 0, "pageIdx": 0}
        }])

        # Simuler l'existence du fichier de relation et son contenu
        relation_file = mock_open.return_value.__enter__.return_value
        relation_file.read.return_value = simulated_relation_json

        try:
            # Appel de la méthode loadRelation
            relation = Relation.loadRelation("test_relation", self.disk, self.buffer_manager, "BD1")

            # Vérification des résultats
            self.assertIsNotNone(relation, "Relation.loadRelation a retourné None.")
            self.assertEqual(relation.name, "test_relation", "Le nom de la relation ne correspond pas.")
            self.assertEqual(relation.nb_column, 2, "Le nombre de colonnes ne correspond pas.")
            self.assertEqual(len(relation.columns), 2, "Le nombre de colonnes définies est incorrect.")
            self.assertEqual(relation.columns[0].name, "col1", "Le nom de la première colonne est incorrect.")
            self.assertEqual(relation.columns[1].type.size, 10, "La taille du VarChar de la deuxième colonne est incorrecte.")
            
        except FileNotFoundError as e:
            # Déboguer pour mieux comprendre d'où vient l'erreur
            print(f"Chemin simulé : {e.filename}")  # Afficher le fichier qui pose problème
            self.fail(f"loadRelation a déclenché FileNotFoundError alors que les fichiers étaient simulés : {e}")
            
    def test_writeRecordToBuffer(self):
        record = Record(values=[123, "test"])
        buffer = Buffer(size=self.config.pagesize)
        pos = 0

        result = self.relation.writeRecordToBuffer(record, buffer, pos)
        self.assertEqual(result, 20)  # 4 bytes for int + 4 bytes for offset + 10 bytes for varchar + 2 bytes for offset

    def test_readFromBuffer(self):
        record = Record(values=[])
        buffer = Buffer(size=self.config.pagesize)
        pos = 0

        # Write data to buffer
        self.relation.writeRecordToBuffer(Record(values=[123, "test"]), buffer, pos)

        result = self.relation.readFromBuffer(record, buffer, pos)
        self.assertEqual(result, 20)  # 4 bytes for int + 4 bytes for offset + 10 bytes for varchar + 2 bytes for offset
        self.assertEqual(record.values, [123, "test"])

    @patch.object(DiskManager, 'AllocPage', return_value=PageId(0, 1))
    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(return_value=0), put_int=MagicMock()))
    def test_addDataPage(self, mock_get_page, mock_alloc_page):
        dataPageId = self.relation.addDataPage()
        self.assertEqual(dataPageId, PageId(0, 1))

    @patch.object(Relation, 'get_nbDataPage', return_value=1)
    @patch.object(Relation, 'get_dataPageInfo', return_value=(PageId(0, 1), 100))
    @patch.object(Relation, 'has_freeSlot', return_value=True)
    def test_getFreeDataPageId(self, mock_has_free_slot, mock_get_data_page_info, mock_get_nb_data_page):
        dataPageId = self.relation.getFreeDataPageId(50)
        self.assertEqual(dataPageId, PageId(0, 1))

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(return_value=1)))
    def test_get_nbDataPage(self, mock_get_page):
        nb_dataPage = self.relation.get_nbDataPage()
        self.assertEqual(nb_dataPage, 1)

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[0, 1, 100])))
    def test_get_dataPageInfo(self, mock_get_page):
        pageId, remaining_dataPageSize = self.relation.get_dataPageInfo(0)
        self.assertEqual(pageId, PageId(0, 1))
        self.assertEqual(remaining_dataPageSize, 100)

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(
        read_int=MagicMock(side_effect=[-1] * 10)  # Tous les slots sont libres
    ))
    @patch.object(Relation, 'writeRecordToBuffer', return_value=20)  # Taille du record
    def test_writeRecordToDataPage(self, mock_write_record_to_buffer, mock_get_page):
        record = Record(values=[123, "test"])
        dataPageId = PageId(0, 1)
        recordId = self.relation.writeRecordToDataPage(record, dataPageId)

        self.assertEqual(recordId.slotIdx, 0)
        self.assertEqual(recordId.pageId, dataPageId)

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[1, 0, 1, 100])))
    def test_updateHeaderPage(self, mock_get_page):
        pageId = PageId(0, 1)
        tailleRecord = 20
        self.relation.updateHeaderPage(pageId, tailleRecord)
        
    @patch.object(Buffer, 'read_int', side_effect=[0, -1]) 
    def test_updateDataPage(self, mock_read_int):
        buffer = Buffer(size=self.config.pagesize)
        positionRecord = 0
        tailleRecord = 20
        recordId = RecordId(PageId(0, 1))

        self.relation.updateDataPage(buffer, positionRecord, tailleRecord, recordId)
        mock_read_int.assert_called()  # Vérifie que la méthode a été appelée

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[0, -1])))  # 1 enregistrement, suivi d'aucun
    @patch.object(Relation, 'readFromBuffer', return_value=20)
    def test_getRecordsInDataPage(self, mock_read_from_buffer, mock_get_page):
        dataPageId = PageId(0, 1)
        records = self.relation.getRecordsInDataPage(dataPageId)
        self.assertEqual(len(records), 1)

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[1, 0, 1, -1])))
    def test_getDataPages(self, mock_get_page):
        dataPages = self.relation.getDataPages()
        self.assertEqual(len(dataPages), 1)
        self.assertEqual(dataPages[0], PageId(0, 1))

    @patch.object(Relation, 'getFreeDataPageId', return_value=PageId(0, 1))
    @patch.object(Relation, 'writeRecordToDataPage', return_value=RecordId(PageId(0, 1)))
    def test_InsertRecord(self, mock_write_record_to_data_page, mock_get_free_data_page_id):
        record = Record(values=[123, "test"])
        recordId = self.relation.InsertRecord(record)
        self.assertEqual(recordId.pageId, PageId(0, 1))

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[-1, 0])))
    def test_has_freeSlot(self, mock_get_page):
        dataPageId = PageId(0, 1)
        has_free_slot = self.relation.has_freeSlot(dataPageId)
        self.assertTrue(has_free_slot)

    def test_getRecordSize(self):
        record = Record(values=[123, "test"])
        size = self.relation.getRecordSize(record)
        self.assertEqual(size, 20)  

    @patch.object(Relation, 'getDataPages', return_value=[PageId(0, 1)])
    @patch.object(Relation, 'getRecordsInDataPage', return_value=[Record(values=[123, "test"])])
    def test_GetAllRecords(self, mock_get_records_in_data_page, mock_get_data_pages):
        records = self.relation.GetAllRecords()
        self.assertEqual(len(records), 1)  
        self.assertEqual(records[0].values, [123, "test"])

    @patch.object(BufferManager, 'getPage', return_value=MagicMock(read_int=MagicMock(side_effect=[1, 0, 1, -1])))
    @patch.object(DiskManager, 'DeAllocPage', return_value=None)
    def test_desallocAllPagesOfRelation(self, mock_dealloc_page, mock_get_page):
        self.relation.desallocAllPagesOfRelation()
        # Verify that all pages were deallocated

if __name__ == '__main__':
    unittest.main()
