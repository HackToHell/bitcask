import binascii
import datetime
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass


#Bitcask API from the paper
class CaskDB(ABC):
    @abstractmethod
    def open(self, location):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    # @abstractmethod
    # def delete(self, key):
    #     pass
    #
    # @abstractmethod
    # def list_keys(self):
    #     pass

class CaskDBImpl(CaskDB):
    def __init__(self):
        self.kh = {}
        self.fs_file = None

    def put(self, key, value):
        if self.fs_file is None:
            raise Exception("File not yet open")
        if not self.fs_file.can_append():
            #refresh
            self.fs_file = FSFile(location='/tmp')
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        chunk = Chunk(timestamp, key, value).serialize()
        offset = self.fs_file.append_file(chunk)
        if offset == -1:
            return False
        self.kh[key] = Cache(self.fs_file.file_name, len(value), offset, str(timestamp))
        return True


    def open(self, location):
        self.location = location
        self.fs_file = FSFile(location='/tmp')

    def get(self, key):
        if key not in self.kh:
            print(self.kh)
            raise Exception("not yet implemented")
        cc = self.kh[key]
        if cc.file_id != self.fs_file.file_name:
            raise Exception("not yet implemented")
        line = self.fs_file.read_file(cc.value_pos)
        rkey, rvalue = Chunk.read(line)
        if key != rkey:
            raise Exception("key mismatch corruption")
        return rvalue

@dataclass
class Cache:
    file_id: str
    value_size: int
    value_pos :int
    timestamp: str

class FSFile:
    def __init__(self,location, max_size=1000):
        self.counter = 0
        self.offset = 0
        self.max_size = max_size
        time = datetime.datetime.now(datetime.timezone.utc)
        self.file_name = f'{location}/{time.year}/{time.month}{time.day}/{random.randint(0,10000)}.chunk'

    def can_append(self) -> bool:
        return self.counter <= self.max_size

    def append_file(self, line) -> int:
        curr_offset = self.offset
        if self.counter >= self.max_size:
            return -1
        folder = '/'.join(self.file_name.split('/')[:-1])
        os.makedirs(folder, exist_ok=True)
        with open(self.file_name, 'a+') as fp:
            self.counter += 1
            fp.write(line)
            self.offset += len(line)
        return curr_offset

    def read_file(self, offset):
        with open(self.file_name, 'rb', 0)  as fp:
            fp.seek(offset)
            return fp.readline().decode('utf-8').strip()

    def get_file_name(self):
        return self.file_name

class Chunk:
    def __init__(self, timestamp, key, value):
        self.timestamp = timestamp
        self.key_len = len(str(key))
        self.value_len = len(value)
        self.key = key
        self.value = value

    def serialize(self):
        #guess what binaries suck
        chunk = f'{self.timestamp}|{self.key_len}|{self.value_len}|{self.key}|{self.value}'
        crc = binascii.crc32(chunk.encode('utf8'))
        cchunk = f'{crc}|{chunk}'
        return cchunk

    @staticmethod
    def read(row):
        crc, chunk = row.split('|', 1)
        if int(crc) != binascii.crc32(chunk.encode('utf-8')):
            raise Exception(f"CRC32 no match {crc} : {chunk}")
        _, key_len, value_len, kv = chunk.split('|', 3)
        key_len = int(key_len)
        value_len = int(value_len)
        key = kv[:key_len]
        value = kv[key_len+1:value_len]
        return key, value



if __name__ == '__main__':
    x = CaskDBImpl()
    x.open('/tmp')
    print(x.put('1','test'))
    print(x.get('1'))