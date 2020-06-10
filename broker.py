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

        self.streams_partitions_numbers_lock = Lock()
        self.streams_partitions_numbers = {}

        self.stream_assignments_lock = Lock()
        self.stream_assignments = {}

        self.partitions_lock = Lock()
        self.partitions = {}

        Thread(target=self._rebalance_loop, daemon=True).start()

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
                receiver = data['params']['receiver']
                instance = data['params']['instance']
                response.data = self.get_message(stream=stream, receiver=receiver, instance=instance)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        if method == 'commit_message':
            try:
                stream = data['params']['stream']
                partition_number = data['params']['partition']
                index = data['params']['index']
                receiver = data['params']['receiver']
                response.media = self.commit_message(stream=stream,
                                                     partition_number=partition_number,
                                                     index=index,
                                                     receiver=receiver)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        if method == 'set_offset':
            try:
                stream = data['params']['stream']
                partition_number = data['params']['partition']
                index = data['params']['index']
                receiver = data['params']['receiver']
                response.media = self.set_offset(stream=stream,
                                                 partition_number=partition_number,
                                                 index=index,
                                                 receiver=receiver)
            except KeyError:
                response.status = falcon.HTTP_400
            finally:
                return

        response.status = falcon.HTTP_400

    @log_errors
    def put_message(self, key: str, value: bytes, stream: str, partition_number: int = None) -> dict:
        # WITH LOCK REBALANCE, FALTA HILO REBALANCEO AUTOMÁTICO + PING

        partition_numbers = self._get_stream_partition_numbers(stream)

        if partition_number is None:
            partition_number = utils.get_partition_number(partition_numbers, key)

        elif partition_number not in partition_numbers:
            raise ValueError('partition does not exist')

        partition = self._get_partition(stream=stream, partition_number=partition_number)

        timestamp = utils.get_timestamp_ms()
        item = PartitionItem(value, timestamp)

        index = partition.put(item)

        return {'stream': stream, 'partition': partition_number, 'index': index, 'timestamp': timestamp, 'status': 'ok'}

    @log_errors
    def get_message(self, stream: str, receiver: str, instance: str) -> dict:
        with self.stream_assignments_lock:
            if stream not in self.stream_assignments:
                self.stream_assignments[stream] = {}

            if receiver not in self.stream_assignments[stream]:
                self.stream_assignments[stream][receiver] = {}

            do_rebalance = False
            if instance not in self.stream_assignments[stream][receiver]:
                self.stream_assignments[stream][receiver][instance] = {'partitions': []}
                do_rebalance = True
            self.stream_assignments[stream][receiver][instance]['last_seen'] = utils.get_timestamp_ms()

        if do_rebalance:
            self._rebalance(stream, receiver)

        instance_partition_numbers = list(self.stream_assignments[stream][receiver][instance]['partitions'])

        done = False
        while not done:
            number_of_partitions = len(instance_partition_numbers)
            if number_of_partitions == 0:
                done = True
                continue

            partition_index = random.randint(0, number_of_partitions - 1)
            partition_number = instance_partition_numbers.pop(partition_index)

            partition = self._get_partition(stream, partition_number)

            item = partition.get(receiver)
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
    def commit_message(self, stream: str, partition_number: int, index: int, receiver: str) -> dict:
        try:
            self._get_partition(stream, partition_number).commit(index, receiver)
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
    def set_offset(self, stream: str, partition_number: int, index: int, receiver: str) -> dict:
        self._get_partition(stream, partition_number).set_offset(receiver, index)
        return {'stream': stream, 'partition': partition_number, 'index': index, 'status': 'ok'}

    @log_errors
    def create_stream(self, stream: str, config):
        # Create a stream with a custom configuration
        raise NotImplementedError

    @log_errors
    def update_stream(self, stream: str, config):
        # Increase number of partitions
        raise NotImplementedError

    def _get_partition(self, stream: str, partition_number: int):
        with self.partitions_lock:
            if stream not in self.partitions:
                self.partitions[stream] = {}

            if partition_number not in self.partitions[stream]:
                self.partitions[stream][partition_number] = Partition(stream=stream,
                                                                      number=partition_number,
                                                                      data_dir=self.config['global']['data_dir'])
            return self.partitions[stream][partition_number]

    def _get_stream_partition_numbers(self, stream: str):
        if stream in self.streams_partitions_numbers:
            return self.streams_partitions_numbers[stream]

        with self.streams_partitions_numbers_lock:
            partitions_numbers = []
            self.streams_partitions_numbers[stream] = partitions_numbers

            stream_exists = False
            stream_path = f"{self.config['global']['data_dir']}/streams/{stream}/"
            if path.isdir(stream_path):
                for partition_number in sorted(listdir(stream_path)):
                    try:
                        partitions_numbers.append(int(partition_number))
                        stream_exists = True
                    except ValueError:
                        continue

            if not stream_exists:
                for partition_number in range(0, self.config['streams']['default']['partitions']):
                    # Initialize new partitions
                    Partition(stream=stream, number=partition_number, data_dir=self.config['global']['data_dir'])
                    partitions_numbers.append(partition_number)

            return self.streams_partitions_numbers[stream]

    def _rebalance_loop(self):
        while True:
            for stream in self.stream_assignments.keys():
                for receiver in self.stream_assignments[stream].keys():
                    self._rebalance(stream, receiver)
            time.sleep(self.config['global']['rebalance_interval'])

    def _rebalance(self, stream, receiver):
        with self.stream_assignments_lock:
            stream_receiver_instances = []
            for instance, value in self.stream_assignments[stream][receiver].items():
                instance_unseen_time = (utils.get_timestamp_ms() - value['last_seen']) / 1000
                if instance_unseen_time < self.config['global']['receiver_instance_timeout']:
                    stream_receiver_instances.append(instance)
                else:
                    self.stream_assignments[stream][receiver][instance]['partitions'] = []

            stream_partition_numbers = self._get_stream_partition_numbers(stream)

            number_of_partitions = len(stream_partition_numbers)
            number_of_instances = len(stream_receiver_instances)
            if number_of_instances > number_of_partitions:
                number_of_partitions = number_of_instances

            if not number_of_instances:
                return

            step = number_of_partitions // number_of_instances
            remainder = number_of_partitions % number_of_instances

            for index in range(0, number_of_partitions - remainder, step):
                instance_index = index // step
                self.stream_assignments[stream][receiver][
                    stream_receiver_instances[instance_index]]['partitions'] = stream_partition_numbers[index:index +
                                                                                                        step]

            for index in range(number_of_partitions - remainder, number_of_partitions):
                instance_index = index - number_of_partitions + 1
                self.stream_assignments[stream][receiver][
                    stream_receiver_instances[instance_index]]['partitions'].append(stream_partition_numbers[index])

            logging.info(json.dumps(self.stream_assignments, indent=2))