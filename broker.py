#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import listdir, path
from threading import Lock, Thread
import traceback
import random
import falcon
from partition import Partition, PartitionItem
import utils
import time
import json
import logging


def log_errors(method):
    def _try_except(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            raise e

    return _try_except


class Broker:
    def __init__(self, config):
        self.config = config

        self.partitions_by_stream_lock = Lock()
        self.partitions_by_stream = {}

        self.partitions_by_receiver_group_lock = Lock()
        self.partitions_by_receiver_group = {}

        self.partition_locks = {}
        self.partitions = {}

        Thread(target=self._rebalance_loop, daemon=True).start()
        Thread(target=self._prune_loop, daemon=True).start()

    def on_post(self, request, response):
        data = utils.unpack(utils.decompress(request.stream.read()))

        if 'method' not in data:
            response.status = falcon.HTTP_400
            return
        method = data['method']

        if method == 'put_message':
            try:
                try:
                    key = data['params']['key']
                except KeyError:
                    key = None
                value = data['params']['value']
                stream = data['params']['stream']
                try:
                    partition_number = data['params']['partition']
                except KeyError:
                    partition_number = None
                response.media = self.put_message(key=key,
                                                  value=value,
                                                  stream=stream,
                                                  partition_number=partition_number)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        if method == 'get_message':
            try:
                stream = data['params']['stream']
                receiver_group = data['params']['receiver_group']
                receiver = data['params']['receiver']
                response.data = self.get_message(stream=stream,
                                                 receiver_group=receiver_group,
                                                 receiver=receiver)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        if method == 'commit_message':
            try:
                stream = data['params']['stream']
                partition_number = data['params']['partition']
                index = data['params']['index']
                receiver_group = data['params']['receiver_group']
                response.media = self.commit_message(stream=stream,
                                                     partition_number=partition_number,
                                                     index=index,
                                                     receiver_group=receiver_group)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        if method == 'set_offset':
            try:
                stream = data['params']['stream']
                partition_number = data['params']['partition']
                index = data['params']['index']
                receiver_group = data['params']['receiver_group']
                response.media = self.set_offset(stream=stream,
                                                 partition_number=partition_number,
                                                 index=index,
                                                 receiver_group=receiver_group)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        response.status = falcon.HTTP_400

    @log_errors
    def put_message(self,
                    key: str,
                    value: bytes,
                    stream: str,
                    partition_number: int = None) -> dict:
        partition_numbers = self._get_stream_partition_numbers(stream)

        if partition_number is None:
            partition_number = utils.get_partition_number(partition_numbers, key)

        elif partition_number not in partition_numbers:
            raise ValueError('partition does not exist')

        partition = self._get_partition(stream=stream, partition_number=partition_number)

        timestamp = utils.get_timestamp_ms()
        item = PartitionItem(value, timestamp)

        index = partition.put(item)

        return {
            'stream': stream,
            'partition': partition_number,
            'index': index,
            'timestamp': timestamp,
            'status': 'ok'
        }

    @log_errors
    def get_message(self, stream: str, receiver_group: str, receiver: str) -> dict:
        with self.partitions_by_receiver_group_lock:
            if stream not in self.partitions_by_receiver_group:
                self.partitions_by_receiver_group[stream] = {}

            if receiver_group not in self.partitions_by_receiver_group[stream]:
                self.partitions_by_receiver_group[stream][receiver_group] = {}

            if receiver not in self.partitions_by_receiver_group[stream][receiver_group]:
                self.partitions_by_receiver_group[stream][receiver_group][receiver] = {
                    'partitions': []
                }

            self.partitions_by_receiver_group[stream][receiver_group][receiver][
                'last_seen'] = utils.get_timestamp_ms()

        receiver_partition_numbers = list(
            self.partitions_by_receiver_group[stream][receiver_group][receiver]['partitions'])

        if len(receiver_partition_numbers) == 0:
            return utils.compress(
                utils.pack({
                    'stream': stream,
                    'status': 'all_partitions_assigned'
                }))

        done = False
        while not done:
            number_of_partitions = len(receiver_partition_numbers)
            if number_of_partitions == 0:
                done = True
                continue

            partition_index = random.randint(0, number_of_partitions - 1)
            partition_number = receiver_partition_numbers.pop(partition_index)

            partition = self._get_partition(stream, partition_number)

            item = partition.get(receiver_group)
            if item is None:
                continue

            return utils.compress(
                utils.pack({
                    'stream': stream,
                    'partition': partition_number,
                    'index': item['index'],
                    'value': item['value'],
                    'timestamp': item['timestamp'],
                    'status': 'ok'
                }))

        return utils.compress(utils.pack({'stream': stream, 'status': 'end_of_stream'}))

    @log_errors
    def commit_message(self, stream: str, partition_number: int, index: int,
                       receiver_group: str) -> dict:
        try:
            self._get_partition(stream, partition_number).commit(index, receiver_group)
            return {'stream': stream, 'partition': partition_number, 'index': index, 'status': 'ok'}
        except ValueError as error:
            return {
                'stream': stream,
                'partition': partition_number,
                'index': index,
                'status': 'error',
                'message': str(error)
            }

    @log_errors
    def set_offset(self, stream: str, partition_number: int, index: int,
                   receiver_group: str) -> dict:
        self._get_partition(stream, partition_number).set_offset(receiver_group, index)
        return {'stream': stream, 'partition': partition_number, 'index': index, 'status': 'ok'}

    def _get_partition(self, stream: str, partition_number: int):
        with self._get_partition_lock(stream, partition_number):
            if stream not in self.partitions:
                self.partitions[stream] = {}

            if partition_number not in self.partitions[stream]:
                self.partitions[stream][partition_number] = Partition(
                    stream=stream,
                    number=partition_number,
                    data_dir=self.config['global']['data_dir'])
            return self.partitions[stream][partition_number]

    def _get_stream_path(self, stream: str) -> str:
        return f"{self.config['global']['data_dir']}/streams/{stream}/"

    def _get_stream_partition_numbers(self, stream: str):
        if stream in self.partitions_by_stream:
            return self.partitions_by_stream[stream]

        with self.partitions_by_stream_lock:
            partition_numbers = []
            self.partitions_by_stream[stream] = partition_numbers

            stream_exists = False
            stream_path = self._get_stream_path(stream)
            if path.isdir(stream_path):
                for partition_number in sorted(listdir(stream_path)):
                    try:
                        partition_numbers.append(int(partition_number))
                        stream_exists = True
                    except ValueError:
                        continue

            if not stream_exists:
                if 'streams' in self.config and stream in self.config['streams']:
                    partitions_to_create = self.config['streams'][stream]['partitions']
                else:
                    partitions_to_create = self.config['global']['partitions']

                for partition_number in range(0, partitions_to_create):
                    # Initialize new partitions
                    Partition(stream=stream,
                              number=partition_number,
                              data_dir=self.config['global']['data_dir'])
                    partition_numbers.append(partition_number)

            return self.partitions_by_stream[stream]

    def _rebalance_loop(self):
        while True:
            self._rebalance()
            logging.debug(
                f"next rebalance will happen in {self.config['global']['rebalance_interval']} seconds"
            )
            time.sleep(self.config['global']['rebalance_interval'])

    def _rebalance(self):
        with self.partitions_by_receiver_group_lock:
            logging.debug(f'rebalancing...')

            logging.info(json.dumps(self.partitions_by_receiver_group, indent=4))

            receivers_to_remove = []
            for stream in self.partitions_by_receiver_group.keys():
                for receiver_group in self.partitions_by_receiver_group[stream].keys():
                    stream_receiver_group_receivers = []
                    for receiver, value in self.partitions_by_receiver_group[stream][
                            receiver_group].items():
                        logging.info(f'{receiver} {receiver_group}')
                        receiver_unseen_time = (utils.get_timestamp_ms() -
                                                value['last_seen']) / 1000
                        if receiver_unseen_time < self.config['global']['receiver_timeout']:
                            stream_receiver_group_receivers.append(receiver)
                        else:
                            receivers_to_remove.append((stream, receiver_group, receiver))
                            logging.info(
                                f'receiver {receiver} kicked from the receiver_group {receiver_group} ({stream})'
                            )

                    stream_partition_numbers = self._get_stream_partition_numbers(stream)

                    number_of_partitions = len(stream_partition_numbers)
                    number_of_receivers = len(stream_receiver_group_receivers)

                    if number_of_receivers > number_of_partitions:
                        number_of_partitions = number_of_receivers

                    if not number_of_receivers:
                        continue

                    step = number_of_partitions // number_of_receivers
                    remainder = number_of_partitions % number_of_receivers

                    for index in range(0, number_of_partitions - remainder, step):
                        receiver_index = index // step
                        self.partitions_by_receiver_group[stream][receiver_group][
                            stream_receiver_group_receivers[receiver_index]][
                                'partitions'] = stream_partition_numbers[index:index + step]

                    for index in range(number_of_partitions - remainder, number_of_partitions):
                        receiver_index = index - number_of_partitions + 1
                        self.partitions_by_receiver_group[stream][receiver_group][
                            stream_receiver_group_receivers[receiver_index]]['partitions'].append(
                                stream_partition_numbers[index])

            for stream, receiver_group, receiver in receivers_to_remove:
                del self.partitions_by_receiver_group[stream][receiver_group][receiver]

    def _get_partition_lock(self, stream: str, partition_number: int) -> Lock:
        if stream not in self.partition_locks:
            self.partition_locks[stream] = {}

        if partition_number not in self.partition_locks[stream]:
            self.partition_locks[stream][partition_number] = Lock()

        return self.partition_locks[stream][partition_number]

    def _prune_loop(self):
        while True:
            time.sleep(self.config['global']['prune_interval'])
            streams_path = f"{self.config['global']['data_dir']}/streams/"
            for stream in listdir(streams_path):
                stream_path = self._get_stream_path(stream)
                if path.isdir(stream_path):
                    partition_numbers = []
                    for partition_number in sorted(listdir(stream_path)):
                        try:
                            partition_numbers.append(int(partition_number))
                        except ValueError:
                            continue

                    stream_with_defined_ttl = 'streams' in self.config and stream in self.config[
                        'streams'] and 'ttl' in self.config['streams'][stream]

                    if stream_with_defined_ttl:
                        ttl = self.config['streams'][stream]['ttl']
                    else:
                        ttl = self.config['global']['ttl']

                    for partition_number in partition_numbers:
                        logging.debug(f'pruning partition {partition_number} of stream {stream}...')
                        self._get_partition(stream, partition_number).prune(int(ttl))