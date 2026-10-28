#!/usr/bin/env python
# -*- coding: utf-8 -*-

import msgpack
import snappy
import cramjam
import time
import random
import hashlib
import logging
import json
from pathlib import Path


def stream_path(data_dir, stream):
    if (not isinstance(stream, str) or not stream or stream in ('.', '..')
            or '/' in stream or '\\' in stream
            or any(ord(char) < 32 or ord(char) == 127 for char in stream)):
        raise ValueError('Invalid stream name')
    root = (Path(data_dir) / 'streams').resolve()
    target = (root / stream).resolve()
    if not target.is_relative_to(root):
        raise ValueError('Stream path escapes the storage directory')
    return target


def validate_partition(number):
    if type(number) is not int or number < 0:
        raise ValueError('Partition must be a non-negative integer')


def pack(message: dict) -> bytes:
    return msgpack.packb(message)


def unpack(message: bytes) -> dict:
    return msgpack.unpackb(message)


def compress(message: bytes) -> bytes:
    return snappy.compress(message)


def decompress(message: bytes) -> bytes:
    return snappy.decompress(message)


class RequestTooLarge(ValueError):
    pass


def decode_request(body, max_bytes=None):
    if max_bytes is not None and len(body) > max_bytes:
        raise RequestTooLarge('Request body exceeds the size limit')
    oversized = False
    try:
        if max_bytes is not None and cramjam.snappy.decompress_raw_len(body) > max_bytes:
            oversized = True
            raise RequestTooLarge('Uncompressed request exceeds the size limit')
        return unpack(decompress(body)), False
    except Exception:
        try:
            return json.loads(body), True
        except (ValueError, UnicodeError, RecursionError):
            if oversized:
                raise RequestTooLarge('Uncompressed request exceeds the size limit')
            return None, False


def get_timestamp_ms() -> int:
    return int(round(time.time() * 1000))


def string_to_sha3_256(text: str):
    return hashlib.sha3_256(text.encode('utf-8')).hexdigest()


# Max 1024 partitions for key-based partition assignment
def get_partition_number(partition_numbers, key=None):
    # Random partition
    if key is None:
        partition = random.sample(partition_numbers, 1)[0]

    else:
        value = 0
        key_hash = string_to_sha3_256(key)
        for digit in key_hash:
            value += int(digit, 16)
        partition_index = value % len(partition_numbers)
        partition = partition_numbers[partition_index]

    return partition


def log_dict(
    config,
    prefix=None,
    key_prefix=None,
):
    if prefix is None:
        prefix = ''

    if key_prefix is None:
        key_prefix = ''

    for key, value in config.items():
        if isinstance(value, dict):
            if key == 'auth':
                value = dict(map(lambda k: (k, '<hidden>'), value.keys()))
            log_dict(
                config=value,
                prefix=prefix,
                key_prefix=f'{key_prefix}{key}.',
            )
        else:
            logging.info(f'{prefix}{key_prefix}{key}={value}')
