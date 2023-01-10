#!/usr/bin/env python
# -*- coding: utf-8 -*-

import msgpack
import snappy
import time
import random
import hashlib
import logging


def pack(message: dict) -> bytes:
    return msgpack.packb(message)


def unpack(message: bytes) -> dict:
    return msgpack.unpackb(message)


def compress(message: bytes) -> bytes:
    return snappy.compress(message)


def decompress(message: bytes) -> bytes:
    return snappy.decompress(message)


def get_timestamp_ms() -> int:
    return int(round(time.time() * 1000))


def string_to_sha3_256(text: str):
    return hashlib.sha3_256(text.encode('utf-8')).hexdigest()


def get_padded_string(string, prefix='', size=64):
    zeros = size - len(string) - len(prefix)
    if zeros < 0:
        raise ValueError
    padded_string = f"{prefix}{zeros * '0'}{string}"
    return padded_string


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
