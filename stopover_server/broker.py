#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .version import __version__
from .partition import Partition, PartitionItem
from . import utils
from os import listdir, path
from threading import Lock, Thread
import traceback
import random
import falcon
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
        logging.info(f'config: {json.dumps(config, indent=2)}')

        self.partitions_by_stream_lock = Lock()
        self.partitions_by_stream = {}

        self.last_seen_by_group_lock = Lock()
        self.last_seen_by_group = {}

        self.partitions_by_group_lock = Lock()
        self.partitions_by_group = {}

        self.partitions_lock = Lock()
        self.partitions = {}

        Thread(target=self._rebalance_loop, daemon=True).start()
        Thread(target=self._prune_loop, daemon=True).start()

    @staticmethod
    def on_get(request, response):
        response.content_type = 'text/html; charset=utf-8'
        response.body = f'Labteral Stopover {__version__}'

    def on_post(self, request, response):
        bin_data = request.stream.read()

        if bin_data[:1] == b'{':
            # JSON
            data = json.loads(bin_data)
        else:
            # MessagePack
            data = utils.unpack(utils.decompress(bin_data))

        if 'method' not in data:
            response.status = falcon.status_codes.HTTP_400
            return
        method = data['method']
        params = data['params']

        try:
            if method == 'knock':
                self.knock(params)

            elif method == 'put_message':
                response.media = self.put_message(params)

            elif method == 'get_message':
                response.data = self.get_message(params)

            elif method == 'commit_message':
                response.media = self.commit_message(params)

            elif method == 'set_offset':
                response.media = self.set_offset(params)

        except KeyError:
            response.status = falcon.status_codes.HTTP_400

        except Exception:
            response.status = falcon.status_codes.HTTP_500

    @log_errors
    def put_message(self, params: dict) -> dict:
        key = None if 'key' not in params else params['key']
        value = params['value']
        stream = params['stream']
        partition_number = None if 'partition' not in params \
            else params['partition']

        partition_numbers = self._get_stream_partition_numbers(stream)
        if partition_number is None:
            partition_number = \
                utils.get_partition_number(partition_numbers, key)
        elif partition_number not in partition_numbers:
            raise ValueError('partition does not exist')

        timestamp = utils.get_timestamp_ms()
        item = PartitionItem(value, timestamp)

        partition = self._get_partition(stream, partition_number)
        index = partition.put(item)

        return {
            'stream': stream,
            'partition': partition_number,
            'index': index,
            'timestamp': timestamp,
            'status': 'ok'
        }

    @log_errors
    def knock(self, params: dict, do_log=True):
        receiver_group = params['receiver_group']
        receiver = params['receiver']

        with self.last_seen_by_group_lock:
            if receiver_group not in self.last_seen_by_group:
                self.last_seen_by_group[receiver_group] = {}
            self.last_seen_by_group[receiver_group][receiver] \
                = utils.get_timestamp_ms()

        if do_log:
            logging.info(f'{receiver_group}/{receiver} is knocking')

    @log_errors
    def get_message(self, params: dict) -> dict:
        stream = params['stream']
        receiver_group = params['receiver_group']
        receiver = params['receiver']

        self.knock(params, do_log=False)

        with self.partitions_by_group_lock:
            if stream not in self.partitions_by_group:
                self.partitions_by_group[stream] = {}

            if receiver_group not in self.partitions_by_group[stream]:
                self.partitions_by_group[stream][receiver_group] = {}

            if receiver not in self.partitions_by_group[stream][receiver_group]:
                self.partitions_by_group[stream][receiver_group][receiver] = []

            receiver_partition_numbers = list(
                self.partitions_by_group[stream][receiver_group][receiver])

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

        bin_data = utils.pack({'stream': stream, 'status': 'end_of_stream'})
        return utils.compress(bin_data)

    @log_errors
    def commit_message(self, params: dict) -> dict:
        stream = params['stream']
        partition_number = params['partition']
        index = params['index']
        receiver_group = params['receiver_group']

        try:
            partition = self._get_partition(stream, partition_number)
            partition.commit(index, receiver_group)

            return {
                'stream': stream,
                'partition': partition_number,
                'index': index,
                'status': 'ok'
            }

        except ValueError as error:
            return {
                'stream': stream,
                'partition': partition_number,
                'index': index,
                'status': 'error',
                'message': str(error)
            }

    @log_errors
    def set_offset(self, params: dict) -> dict:
        stream = params['stream']
        partition_number = params['partition']
        index = params['index']
        receiver_group = params['receiver_group']

        partition = self._get_partition(stream, partition_number)
        partition.set_offset(receiver_group, index)

        return {
            'stream': stream,
            'partition': partition_number,
            'index': index,
            'status': 'ok'
        }

    def _get_partition(self, stream: str, partition_number: int):
        with self.partitions_lock:
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

            try:
                partitions_target = self.config['streams'][stream]['partitions']
            except KeyError:
                partitions_target = self.config['global']['partitions']

            stream_path = self._get_stream_path(stream)
            if path.isdir(stream_path):
                for partition_number in sorted(listdir(stream_path)):
                    try:
                        partition_numbers.append(int(partition_number))
                    except ValueError:
                        continue

            existing_partitions = len(partition_numbers)
            if partitions_target > existing_partitions:
                for partition_number in range(existing_partitions,
                                              partitions_target):
                    if partition_number in partition_numbers:
                        raise FileNotFoundError(
                            f'missing partitions among {partition_numbers}')

                    Partition(stream=stream,
                              number=partition_number,
                              data_dir=self.config['global']['data_dir'],
                              create_if_missing=True)
                    partition_numbers.append(partition_number)

            return self.partitions_by_stream[stream]

    def _rebalance_loop(self):
        while True:
            self._rebalance()
            remaining_seconds = self.config['global']['rebalance_interval']
            logging.debug(
                f"next rebalance will hapen in {remaining_seconds} seconds")
            time.sleep(self.config['global']['rebalance_interval'])

    def _rebalance(self):
        with self.partitions_by_group_lock:
            logging.debug('rebalancing...')
            logging.info(
                f'assignments: {json.dumps(self.partitions_by_group, indent=4)}'
            )

            receivers_to_remove = []
            for stream in self.partitions_by_group:

                for receiver_group in self.partitions_by_group[stream].keys():
                    stream_receiver_group_receivers = []

                    for receiver in self.partitions_by_group[stream][
                            receiver_group].keys():
                        receiver_unseen_time = (
                            utils.get_timestamp_ms() -
                            self.last_seen_by_group[receiver_group][receiver]
                        ) / 1000

                        if receiver_unseen_time \
                        < self.config['global']['receiver_timeout']:
                            stream_receiver_group_receivers.append(receiver)

                        else:
                            receivers_to_remove.append(
                                (stream, receiver_group, receiver))

                    stream_partition_numbers = \
                        self._get_stream_partition_numbers(stream)

                    number_of_partitions = len(stream_partition_numbers)
                    number_of_receivers = len(stream_receiver_group_receivers)

                    if number_of_receivers > number_of_partitions:
                        number_of_partitions = number_of_receivers

                    if not number_of_receivers:
                        continue

                    step = number_of_partitions // number_of_receivers
                    remainder = number_of_partitions % number_of_receivers

                    for index in range(0, number_of_partitions - remainder,
                                       step):
                        receiver_index = index // step
                        self.partitions_by_group[stream][receiver_group][
                            stream_receiver_group_receivers[
                                receiver_index]] = stream_partition_numbers[
                                    index:index + step]

                    for index in range(number_of_partitions - remainder,
                                       number_of_partitions):
                        receiver_index = index - number_of_partitions + 1
                        self.partitions_by_group[stream][receiver_group][
                            stream_receiver_group_receivers[
                                receiver_index]].append(
                                    stream_partition_numbers[index])

            for stream, receiver_group, receiver in receivers_to_remove:
                logging.info(f'receiver "{receiver}" kicked from the ' \
                    f'receiver_group "{receiver_group}" ' \
                    f'for the stream "{stream}"')
                del self.partitions_by_group[stream][receiver_group][receiver]
                if receiver in self.last_seen_by_group[receiver_group]:
                    del self.last_seen_by_group[receiver_group][receiver]

            # Remove empty groups
            groups_to_remove = []
            for stream in self.partitions_by_group:
                for receiver_group in self.partitions_by_group[stream].keys():
                    if not self.partitions_by_group[stream][receiver_group]:
                        groups_to_remove.append((stream, receiver_group))
            for stream, receiver_group in groups_to_remove:
                del self.partitions_by_group[stream][receiver_group]

            # Remove streams without assignments
            streams_to_remove = []
            for stream in self.partitions_by_group:
                if not self.partitions_by_group[stream]:
                    streams_to_remove.append(stream)
            for stream in streams_to_remove:
                del self.partitions_by_group[stream]

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

                    stream_with_defined_ttl = \
                        'streams' in self.config \
                        and stream in self.config['streams'] \
                        and 'ttl' in self.config['streams'][stream]

                    if stream_with_defined_ttl:
                        ttl = self.config['streams'][stream]['ttl']
                    else:
                        ttl = self.config['global']['ttl']

                    for partition_number in partition_numbers:
                        logging.info(
                            f'pruning stream {stream} ({partition_number})')

                        partition = self._get_partition(stream,
                                                        partition_number)
                        partition.prune(int(ttl))
