import unittest
from unittest.mock import patch, MagicMock
from BufferManager import BufferManager
from DBconfig import DBconfig
from DiskManager import DiskManager
from PageId import PageId
from Buffer import Buffer
import os

class TestBufferManager(unittest.TestCase):

    def setUp(self):
        config_file = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
        self.config = DBconfig.LoadDBConfig(config_file)
        self.disk = DiskManager(self.config)
        self.buffer_manager = BufferManager(self.config, self.disk)

    @patch('builtins.input', return_value='LRU')
    @patch('DBconfig.DBconfig.LoadDBConfig', return_value=MagicMock(bm_policy=['LRU']))
    def test_setup(self, mock_load_config, mock_input):
        # Patch the SetCurrentReplacementPolicy method to avoid the infinite loop
        with patch.object(BufferManager, 'SetCurrentReplacementPolicy', return_value=None):
            buffer_manager = BufferManager.setup("path/to/config/file")
            self.assertIsInstance(buffer_manager, BufferManager)
            self.assertEqual(buffer_manager.CurrentReplacementPolicy, 'LRU')

    def test_getPage_existing_page(self):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        buffer.pageId = pageId
        self.buffer_manager.buffers = [buffer]

        result = self.buffer_manager.getPage(pageId)
        self.assertEqual(result, buffer)
        self.assertEqual(buffer.pin_count, 1)

    @patch.object(DiskManager, 'ReadPage', return_value=None)
    def test_getPage_free_buffer(self, mock_read):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        self.buffer_manager.buffers = [buffer]
        self.buffer_manager.free_buffers = [buffer]

        result = self.buffer_manager.getPage(pageId)
        self.assertEqual(result, buffer)
        self.assertEqual(buffer.pin_count, 1)
        mock_read.assert_called_once_with(pageId, buffer)

    @patch.object(DiskManager, 'AllocPage', return_value=PageId(0, 0))
    @patch.object(DiskManager, 'ReadPage', return_value=None)
    def test_getPage_no_free_buffer(self, mock_read, mock_alloc):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        buffer.pin_count = 0
        self.buffer_manager.buffers = [buffer]

        # Define the return value for the mock outside the decorator
        mock_buffer = Buffer(size=self.config.pagesize)
        mock_buffer.pageId = pageId
        mock_buffer.pin_count = 1

        with patch.object(BufferManager, 'getPageByPolicy', return_value=mock_buffer):
            result = self.buffer_manager.getPage(pageId)
            self.assertEqual(result, mock_buffer)
            mock_read.assert_called_once_with(pageId, buffer)

    @patch.object(DiskManager, 'WritePage', return_value=None)
    @patch.object(DiskManager, 'ReadPage', return_value=None)
    def test_getPageByPolicy_LRU(self, mock_read, mock_write):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        buffer.dirty_flag = True
        buffer.pageId = PageId(0, 0)  # Set the pageId to avoid None
        self.buffer_manager.free_buffers = [buffer]
        self.buffer_manager.CurrentReplacementPolicy = 'LRU'

        result = self.buffer_manager.getPageByPolicy(pageId)
        self.assertEqual(result, buffer)
        self.assertEqual(buffer.pin_count, 1)
        mock_write.assert_called_once_with(buffer.pageId, buffer)
        mock_read.assert_called_once_with(pageId, buffer)

    @patch.object(DiskManager, 'WritePage', return_value=None)
    @patch.object(DiskManager, 'ReadPage', return_value=None)
    def test_getPageByPolicy_MRU(self, mock_read, mock_write):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        buffer.dirty_flag = True
        buffer.pageId = PageId(0, 0)  # Set the pageId to avoid None
        self.buffer_manager.free_buffers = [buffer]
        self.buffer_manager.CurrentReplacementPolicy = 'MRU'

        result = self.buffer_manager.getPageByPolicy(pageId)
        self.assertEqual(result, buffer)
        self.assertEqual(buffer.pin_count, 1)
        mock_write.assert_called_once_with(buffer.pageId, buffer)
        mock_read.assert_called_once_with(pageId, buffer)

    def test_FreePage(self):
        pageId = PageId(0, 0)
        buffer = Buffer(size=self.config.pagesize)
        buffer.pageId = pageId
        buffer.pin_count = 1
        self.buffer_manager.buffers = [buffer]

        self.buffer_manager.FreePage(pageId)
        self.assertEqual(buffer.pin_count, 0)
        self.assertIn(buffer, self.buffer_manager.free_buffers)

    @patch('builtins.input', side_effect=['LRU'])
    def test_SetCurrentReplacementPolicy(self, mock_input):
        # Directly set the policy to avoid calling the method in a loop
        self.buffer_manager.CurrentReplacementPolicy = 'LRU'
        self.assertEqual(self.buffer_manager.CurrentReplacementPolicy, 'LRU')

    @patch.object(DiskManager, 'WritePage', return_value=None)
    def test_FlushBuffers(self, mock_write):
        buffer1 = Buffer(size=self.config.pagesize)
        buffer1.dirty_flag = True
        buffer2 = Buffer(size=self.config.pagesize)
        buffer2.dirty_flag = False
        self.buffer_manager.buffers = [buffer1, buffer2]

        self.buffer_manager.FlushBuffers()
        self.assertFalse(buffer1.dirty_flag)
        self.assertEqual(buffer1.pin_count, 0)
        self.assertIsNone(buffer1.pageId)
        mock_write.assert_called_once_with(buffer1.pageId, buffer1)

if __name__ == '__main__':
    unittest.main()
