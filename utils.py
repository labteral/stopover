#!/usr/bin/env python
# -*- coding: utf-8 -*-

import bson
import snappy
import json
import time
import random
import hashlib


def get_padded_string(string, prefix='', size=64):
    zeros = size - len(string) - len(prefix)
    if zeros < 0:
        raise ValueError
    padded_string = f"{prefix}{zeros * '0'}{string}"
    return padded_string


def pack(message: dict) -> bytes:
    return bson.encode(message)


def unpack(message: bytes) -> dict:
    try:
        return bson.decode(message)
    except bson.errors.InvalidBSON:
        return json.loads(message)


def compress(message: bytes) -> bytes:
    return snappy.compress(message)


def decompress(message: bytes) -> bytes:
    try:
        return snappy.decompress(message)
    except snappy.UncompressError:
        return message


def get_timestamp_ms() -> int:
    return int(round(time.time() * 1000))


def get_timestamp() -> int:
    return int(round(time.time()))


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
