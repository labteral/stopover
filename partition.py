#!/usr/bin/env python
# -*- coding: utf-8 -*-
import utils
from os import makedirs
from easyrocks import DB, WriteBatch
from threading import Lock


class PartitionItem:
    def __init__(self, value: bytes = None, timestamp: int = None, item_bytes: bytes = None):
        if item_bytes is not None:
            self.value, self.timestamp = self._load_from_bytes(item_bytes)
        else:
            if timestamp is None:
                raise ValueError('the timestamp was not provided')
            self.value = value
            self.timestamp = timestamp

    @property
    def dict(self):
        return {'value': self.value, 'timestamp': self.timestamp}

    @property
    def bytes(self):
        return utils.pack(self.dict)

    def _load_from_bytes(self, value):
        value = utils.unpack(value)
        return value['value'], value['timestamp']


class Partition:
    def __init__(self, stream: str, number: int, data_dir: str):
        self.lock = Lock()
        self.stream = stream
        self.number = number
        partition_path = f'{data_dir}/streams/{stream}/{self.number}'

        try:
            makedirs(partition_path)
        except FileExistsError:
            pass
        self._store = DB(partition_path)

    def put(self, item: PartitionItem) -> int:
        with self.lock:
            index = self._get_index() + 1
            message_key = self._get_message_key(index)

            write_batch = WriteBatch()
            self._store.put(message_key, item.bytes, write_batch=write_batch)
            self._increase_index(write_batch)
            self._store.commit(write_batch)

            return index

    def get(self, receiver: str) -> dict:
        offset = self._get_offset(receiver)
        index = offset + 1
        message_key = self._get_message_key(index)

        stored_value = self._store.get(message_key)
        if stored_value is None:
            return

        item = PartitionItem(item_bytes=stored_value).dict
        item['index'] = index
        return item

    def commit(self, offset: int, receiver: str):
        if offset < 0:
            raise ValueError('the partition is empty')
        expected_offset = self._get_offset(receiver) + 1
        if offset != expected_offset:
            raise ValueError(f'trying to commit offset {offset} but {expected_offset} was expected')
        self._increase_offset(receiver)

    def set_offset(self, receiver: str, offset: int):
        index = self._get_index()
        if offset >= index:
            offset = index - 1
        offset_key = self._get_offset_key(receiver)
        self._store.put(offset_key, offset)

    def _get_index(self):
        index_key = self._get_index_key()
        index = self._store.get(index_key)
        if index is None:
            index = -1
        return index

    def _increase_index(self, write_batch):
        index_key = self._get_index_key()
        index = self._get_index()
        index += 1
        self._store.put(index_key, index, write_batch=write_batch)

    def _get_offset(self, receiver: str):
        offset_key = self._get_offset_key(receiver)
        offset = self._store.get(offset_key)
        if offset is None:
            offset = -1
        return offset

    def _increase_offset(self, receiver: str):
        offset_key = self._get_offset_key(receiver)
        offset = self._get_offset(receiver)
        offset += 1
        self._store.put(offset_key, offset)

    def _get_index_key(self):
        index_key = utils.get_padded_string('', prefix='_index:')
        return index_key

    def _get_offset_key(self, receiver):
        offset_key = utils.get_padded_string(receiver, prefix='_offset:')
        return offset_key

    def _get_message_key(self, index):
        message_key = utils.get_padded_string(str(index), prefix='message:')
        return message_key

    def scan(self):
        for key, value in self._store.scan():
            print(key, value)