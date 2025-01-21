import unittest
import os
import shutil
import datetime
import binascii
from unittest.mock import patch
from io import StringIO

from bitcask import CaskDBImpl, Chunk, FSFile


class TestCaskDBImpl(unittest.TestCase):
    def setUp(self):
        self.db = CaskDBImpl()
        self.location = "/tmp/test_caskdb"
        self.db.open(self.location)  #Open the DB for each test
        self.test_dir = '/tmp/test_caskdb'

    def tearDown(self):
        #cleanup after test
        shutil.rmtree(self.test_dir, ignore_errors=True)


    def test_put_and_get(self):
        self.assertTrue(self.db.put("key1", "value1"))
        self.assertEqual(self.db.get("key1"), "value1")

        self.assertTrue(self.db.put("key2", "longer value here"))
        self.assertEqual(self.db.get("key2"), "longer value here")


    def test_put_multiple_values_same_key(self):
        self.assertTrue(self.db.put("key1", "value1_first"))
        self.assertTrue(self.db.put("key1", "value1_second"))
        self.assertEqual(self.db.get("key1"), "value1_second")


    def test_put_empty_value(self):
        self.assertTrue(self.db.put("key_empty", ""))
        self.assertEqual(self.db.get("key_empty"), "")


    def test_get_nonexistent_key(self):
      with self.assertRaises(Exception) as context:
          self.db.get("non_existent_key")
      self.assertEqual(str(context.exception), "not yet implemented")


    def test_put_before_open(self):
        db_not_open = CaskDBImpl()
        with self.assertRaises(Exception) as context:
            db_not_open.put("key", "value")
        self.assertEqual(str(context.exception), "File not yet open")

    def test_get_after_open(self):
       self.db.put("key", "value")
       second_db = CaskDBImpl()
       second_db.open(self.location)
       self.assertEqual(second_db.get("key"), "value")


    def test_file_refresh(self):
        db = CaskDBImpl()
        db.open(self.location)
        # Use a mock to limit the file size to 2 entries
        original_max_size = db.fs_file.max_size
        db.fs_file.max_size=2
        self.assertTrue(db.put("key1", "value1"))
        self.assertTrue(db.put("key2", "value2"))
        self.assertFalse(db.put("key3", "value3"))
        self.assertEqual( 1, db.fs_file.counter) # new file
        self.assertEqual(db.get("key1"), "value1")
        self.assertEqual(db.get("key2"), "value2")
        db.fs_file.max_size=original_max_size

    @patch('bitcask.FSFile.append_file', return_value=-1)
    def test_append_fail(self, mock_append):
        self.assertFalse(self.db.put("key", "value"))

class TestChunk(unittest.TestCase):

    def test_chunk_serialize_and_read(self):
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        key = "test_key"
        value = "test_value"
        chunk = Chunk(timestamp, key, value)
        serialized = chunk.serialize()
        read_key, read_value = Chunk.read(serialized)
        self.assertEqual(key, read_key)
        self.assertEqual(value, read_value)

    def test_chunk_serialize_empty_value(self):
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        key = "key_empty"
        value = ""
        chunk = Chunk(timestamp, key, value)
        serialized = chunk.serialize()
        read_key, read_value = Chunk.read(serialized)
        self.assertEqual(key, read_key)
        self.assertEqual(value, read_value)

    def test_chunk_serialize_long_value(self):
      timestamp = datetime.datetime.now(datetime.timezone.utc)
      key = "long_key"
      long_value = "a" * 1000
      chunk = Chunk(timestamp,key,long_value)
      serialized = chunk.serialize()
      read_key, read_value = Chunk.read(serialized)
      self.assertEqual(key, read_key)
      self.assertEqual(long_value, read_value)

    def test_chunk_crc_mismatch(self):
      timestamp = datetime.datetime.now(datetime.timezone.utc)
      key = "key"
      value = "value"
      chunk = Chunk(timestamp, key, value)
      serialized = chunk.serialize()
      parts = serialized.split('|')
      #tamper with the data
      parts[4] = 'x'
      bad_serialized = '|'.join(parts)
      with self.assertRaises(Exception) as context:
          Chunk.read(bad_serialized)
      self.assertTrue("CRC32 no match" in str(context.exception))


class TestFSFile(unittest.TestCase):
    def setUp(self):
        self.location = "/tmp/test_fsfile"
        self.fs_file = FSFile(location=self.location)
        self.test_dir = '/tmp/test_fsfile'

    def tearDown(self):
        #cleanup after test
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_append_and_read_file(self):
        line1 = "test line 1"
        line2 = "test line 2"
        offset1 = self.fs_file.append_file(line1)
        offset2 = self.fs_file.append_file(line2)
        self.assertEqual(self.fs_file.read_file(offset1), line1)
        self.assertEqual(self.fs_file.read_file(offset2), line2)

    def test_append_with_max_size(self):
        small_fs = FSFile(self.location, max_size=2)
        self.assertEqual(small_fs.append_file("line1"), 0)
        self.assertEqual(small_fs.append_file("line2"), len("line1"))
        self.assertEqual(small_fs.append_file("line3"), -1)
        self.assertEqual(small_fs.counter, 2)

    def test_can_append(self):
        fs = FSFile(self.location, max_size=2)
        self.assertTrue(fs.can_append())
        fs.append_file("line1")
        self.assertTrue(fs.can_append())
        fs.append_file("line2")
        self.assertFalse(fs.can_append())

    def test_get_file_name(self):
        self.assertTrue(self.fs_file.get_file_name().startswith(self.location))
        self.assertTrue(self.fs_file.get_file_name().endswith('.chunk'))
        self.assertTrue(os.path.isdir(os.path.dirname(self.fs_file.get_file_name())))




if __name__ == '__main__':
    unittest.main()