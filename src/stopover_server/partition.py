#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import utils
from os import makedirs
from easyrocks import RocksDB, WriteBatch, CompressionType
from easyrocks.utils import int_to_padded_bytes
from threading import Lock
import logging
from typing import Dict

UINT_BYTES = 8
MAX_UINT = 2**(UINT_BYTES * 8) - 1


class PartitionItem:
    def __init__(self,
                 value: bytes = None,
                 timestamp: int = None,
                 item_dict: Dict = None):
        if item_dict is not None:
            self._load_from_dict(item_dict)
        else:
            if timestamp is None:
                raise ValueError('the timestamp was not provided')
            self._value = value
            self._timestamp = timestamp

    @property
    def value(self):
        return self._value

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def dict(self):
        return {'value': self._value, 'timestamp': self._timestamp}

    def _load_from_dict(self, value: Dict):
        self._value = value['value']
        self._timestamp = value['timestamp']


class Partition:
    MESSAGE = b'\x00'
    INDEX = b'\x01'
    OFFSET = b'\x02'

    def __init__(self,
                 stream: str,
                 number: int,
                 data_dir: str,
                 create_if_missing: bool = False):
        self.lock = Lock()
        self.stream = stream
        self.number = number
        partition_path = f'{data_dir}/streams/{stream}/{self.number}'

        try:
            makedirs(partition_path)
        except FileExistsError:
            pass

        opts = {
            'create_if_missing': create_if_missing,
            'compression': CompressionType.lz4_compression,
            'use_fsync': True,
            'paranoid_checks': True,
            'compaction_options_universal': {
                'compression_size_percent': 0,
            }
        }
        self._store = RocksDB(path=partition_path, opts=opts)

    def put(self, item: PartitionItem) -> int:
        with self.lock:
            index = self._get_index() + 1
            message_key = self._get_message_key(index)

            write_batch = WriteBatch()
            self._store.put(message_key, item.dict, write_batch=write_batch)
            self._increase_index(write_batch)
            self._store.commit(write_batch)

            return index

    def get(self, receiver_group: str, index=None) -> dict:
        with self.lock:
            if index is not None:
                receiver_index = index
            else:
                receiver_index = self._get_offset(receiver_group) + 1
            partition_item = self._get_by_index(receiver_index)

            # Fast-forward the offset if messages were pruned
            if index is None and partition_item is None:
                current_index = self._get_index()
                while (partition_item is None
                       and receiver_index < current_index):
                    self._increase_offset(receiver_group)
                    receiver_index = self._get_offset(receiver_group) + 1
                    partition_item = self._get_by_index(receiver_index)

            if partition_item is None:
                return

            partition_item_dict = partition_item.dict
            partition_item_dict['index'] = receiver_index
            return partition_item_dict

    def commit(self, offset: int, receiver: str):
        with self.lock:
            expected_offset = self._get_offset(receiver) + 1
            if offset != expected_offset:
                raise ValueError(f'trying to commit offset {offset} '
                                 f'but expecting {expected_offset}')
            self._increase_offset(receiver)

    def set_offset(self, receiver: str, offset: int):
        with self.lock:
            index = self._get_index()
            if offset >= index:
                offset = index - 1
            offset_key = self._get_offset_key(receiver)
            self._store.put(offset_key, offset)

    def prune(self, ttl: int):
        ttl *= 1000  # milliseconds

        current_timestamp = utils.get_timestamp_ms()
        keys_to_delete = []

        with self.lock:
            for key, value in self._store.scan(prefix=Partition.MESSAGE):

                # Backwards compatibility
                if isinstance(value, bytes):
                    value = utils.unpack(value)

                item_timestamp = PartitionItem(item_dict=value).timestamp
                if current_timestamp - item_timestamp < ttl:
                    break
                keys_to_delete.append(key)

            for key in keys_to_delete:
                logging.debug(f'Deleting {key}')
                self._store.delete(key)

    def _get_by_index(self, index: int) -> bytes:
        message_key = self._get_message_key(index)
        value = self._store.get(message_key)
        if value is None:
            return None

        # Backwards compatibility
        if isinstance(value, bytes):
            value = utils.unpack(value)

        partition_item = PartitionItem(item_dict=value)
        return partition_item

    def _get_index(self) -> int:
        index_key = Partition.INDEX
        index = self._store.get(index_key)
        if index is None:
            index = -1
        return index

    def _get_offset(self, receiver: str) -> int:
        offset_key = self._get_offset_key(receiver)
        offset = self._store.get(offset_key)
        if offset is None:
            offset = -1
        return offset

    def _increase_index(self, write_batch: WriteBatch):
        next_index = self._get_index() + 1
        if next_index > MAX_UINT:
            raise ValueError(next_index)
        index_key = Partition.INDEX
        self._store.put(index_key, next_index, write_batch=write_batch)

    def _increase_offset(self, receiver: str):
        next_offset = self._get_offset(receiver) + 1
        if next_offset > MAX_UINT:
            raise ValueError(next_offset)
        offset_key = self._get_offset_key(receiver)
        self._store.put(offset_key, next_offset)

    @staticmethod
    def _get_offset_key(receiver: str) -> bytes:
        offset_key = Partition.OFFSET + bytes(receiver, 'utf-8')
        return offset_key

    @staticmethod
    def _get_message_key(index: int) -> bytes:
        message_key = Partition.MESSAGE + int_to_padded_bytes(
            index, UINT_BYTES)
        return message_key
