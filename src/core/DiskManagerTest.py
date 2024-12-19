import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
from DBconfig import DBconfig
from PageId import PageId
from Buffer import Buffer
from DiskManager import DiskManager, dbpath, savefile

class TestDiskManager(unittest.TestCase):

    def setUp(self):
        config_file = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
        self.config = DBconfig.LoadDBConfig(config_file)
        self.disk_manager = DiskManager(self.config)
        self.buffer = Buffer()
        self.disk_manager.AllocPage()

    def tearDown(self):
        for filename in os.listdir(dbpath):
            file_path = os.path.join(dbpath, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)

    @patch('builtins.open', new_callable=mock_open)
    def test_alloc_page(self, mock_file):
        pageId = self.disk_manager.AllocPage()
        self.assertEqual(pageId.fileIdx, 0)
        self.assertEqual(pageId.pageIdx, 1)
        mock_file.assert_called_with(os.path.join(dbpath, 'F0.rsdb'), 'r+b')

    @patch('builtins.open', new_callable=mock_open)
    def test_read_page(self, mock_file):
        pageId = PageId(0, 0)
        self.disk_manager.ReadPage(pageId, self.buffer)
        mock_file.assert_called_with(os.path.join(dbpath, 'F0.rsdb'), 'rb')

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.fsync', return_value=None)
    def test_write_page(self, mock_fsync, mock_file):
        pageId = PageId(0, 0)
        self.disk_manager.WritePage(pageId, self.buffer)
        mock_file.assert_called_with(os.path.join(dbpath, 'F0.rsdb'), 'r+b')

    def test_dealloc_page(self):
        pageId = PageId(0, 0)
        self.disk_manager.DeAllocPage(pageId)
        self.assertIn(pageId, self.disk_manager.free_pageIds)

    @patch('builtins.open', new_callable=mock_open)
    def test_save_state(self, mock_file):
        self.disk_manager.SaveState()
        mock_file.assert_called_with(savefile, 'w')

    @patch('builtins.open', new_callable=mock_open)
    def test_load_state(self, mock_file):
        mock_file.return_value.__enter__.return_value.read.return_value = json.dumps({
            "last_created_page": {"fileIdx": 0, "pageIdx": 0},
            "free_pageIds": [{"fileIdx": 0, "pageIdx": 1}]
        })
        self.disk_manager.LoadState()
        self.assertEqual(self.disk_manager.current_pageId, PageId(0, 0))
        self.assertEqual(self.disk_manager.free_pageIds, [PageId(0, 1)])

if __name__ == '__main__':
    unittest.main()
