#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .version import __version__
from .partition import Partition, PartitionItem
from . import utils
from os import listdir, path
from threading import Lock, Thread
from copy import deepcopy
import traceback
import random
import falcon
import time
import json
import logging
import base64
import hmac


class STATUS:
    OK = 20
    ERROR = 50
    END_OF_STREAM = 21
    ALL_PARTITIONS_ASSIGNED = 22


def handle_error(method):

    def _try_except(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as error:
            traceback.print_exc()
            params = args[0]
            return {
                "stream": params["stream"],
                "receiver_group": params["receiver_group"],
                "error": str(error),
                "status": STATUS.ERROR,
            }

    return _try_except


class Broker:

    def __init__(self, config):
        self.config = config
        if "auth" not in config:
            logging.warning(
                "No auth section configured: any client can use the broker."
            )
        utils.log_dict(self.config, prefix="⚙️  ")

        self.partitions_by_stream_lock = Lock()
        self.partitions_by_stream = {}

        self.last_seen_by_group_lock = Lock()
        self.last_seen_by_group = {}

        self.partitions_by_group_lock = Lock()
        self.partitions_by_group = {}
        self._logged_partitions_by_group = {}

        self.partitions_lock = Lock()
        self.partitions = {}

        Thread(target=self._rebalance_loop, daemon=True).start()
        Thread(target=self._prune_loop, daemon=True).start()

    def check_authenticated(self, headers):
        if "authorization" in headers:
            return True
        return False

    def check_authorized(self, headers):
        try:
            scheme, token = headers.get("authorization", "").split()
            if scheme.lower() != "basic":
                return False
            client_id, client_secret = (
                base64.b64decode(token, validate=True)
                .decode("ascii")
                .split(":", 1)
            )
        except (ValueError, UnicodeError):
            return False
        expected = self.config["auth"].get(client_id)
        return expected is not None and hmac.compare_digest(
            client_secret.encode(), str(expected).encode()
        )

    @staticmethod
    def on_get(request, response):
        response.content_type = "text/html; charset=utf-8"
        response.text = f"Labteral Stopover {__version__}"

    def on_post(self, request, response):
        headers = {
            key.lower(): value for (key, value) in request.headers.items()
        }

        if "auth" in self.config:
            is_authenticated = self.check_authenticated(headers)
            if not is_authenticated:
                response.status = falcon.status_codes.HTTP_401
                return

            is_authorized = (
                self.check_authorized(headers) if is_authenticated else False
            )
            if not is_authorized:
                response.status = falcon.status_codes.HTTP_403
                return

        if 'stopover.request' in request.env:
            data, plain_response = request.env['stopover.request']
        else:
            data, plain_response = utils.decode_request(request.bounded_stream.read())

        if (
            not isinstance(data, dict)
            or "method" not in data
            or not isinstance(data.get("params"), dict)
        ):
            response.status = falcon.status_codes.HTTP_400
            return
        method = data["method"]
        params = data["params"]

        try:
            if 'stream' in params:
                self._get_stream_path(params['stream'])
            if params.get('partition') is not None:
                utils.validate_partition(params['partition'])
        except ValueError:
            response.status = falcon.HTTP_400
            return

        try:
            if method == "knock":
                response_data = self.knock(params)

            elif method == "health":
                response_data = {"status": STATUS.OK}

            elif method == "put_message":
                response_data = self.put_message(params)

            elif method == "get_message":
                response_data = self.get_message(params)

            elif method == "get_partitions":
                response_data = self.get_partitions(params)

            elif method == "commit_message":
                response_data = self.commit_message(params)

            elif method == "set_offset":
                response_data = self.set_offset(params)

            else:
                response.status = falcon.status_codes.HTTP_400
                return

            if not plain_response:
                response.data = utils.compress(utils.pack(response_data))
            else:
                response.data = json.dumps(response_data).encode("utf-8")

        except KeyError:
            response.status = falcon.status_codes.HTTP_400
            return

        except Exception:
            response.status = falcon.status_codes.HTTP_500
            return

    @handle_error
    def put_message(self, params: dict) -> dict:
        key = None if "key" not in params else params["key"]
        value = params["value"]
        stream = params["stream"]
        partition_number = (
            None if "partition" not in params else params["partition"]
        )

        partition_numbers = self._get_stream_partition_numbers(stream)
        if partition_number is None:
            partition_number = utils.get_partition_number(
                partition_numbers, key
            )
        elif partition_number not in partition_numbers:
            raise ValueError("partition does not exist")

        timestamp = utils.get_timestamp_ms()
        item = PartitionItem(value, timestamp)

        partition = self._get_partition(stream, partition_number)
        index = partition.put(item)

        return {
            "stream": stream,
            "partition": partition_number,
            "index": index,
            "timestamp": timestamp,
            "status": STATUS.OK,
        }

    @handle_error
    def knock(self, params: dict, do_log=True):
        receiver_group = params["receiver_group"]
        receiver = params["receiver"]

        with self.last_seen_by_group_lock:
            if receiver_group not in self.last_seen_by_group:
                self.last_seen_by_group[receiver_group] = {}
            self.last_seen_by_group[receiver_group][
                receiver
            ] = utils.get_timestamp_ms()

        if do_log:
            logging.debug(f"{receiver_group}/{receiver} is knocking")

        return {
            "receiver_group": receiver_group,
            "receiver": receiver,
            "status": STATUS.OK,
        }

    @handle_error
    def get_message(self, params: dict) -> dict:
        stream = params["stream"]
        receiver_group = params["receiver_group"]
        receiver = params["receiver"]
        index = params["index"] if "index" in params else None
        requested_partition = params.get("partition")
        skip_pruned = params.get("skip_pruned", False)
        if type(skip_pruned) is not bool:
            raise ValueError("skip_pruned must be a boolean")
        for value in (requested_partition, index):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(
                    "partition and index must be non-negative integers"
                )

        self.knock(params, do_log=False)

        receiver_partition_numbers = self._get_receiver_partition_numbers(
            stream,
            receiver_group,
            receiver,
        )

        if len(receiver_partition_numbers) == 0:
            return {
                "stream": stream,
                "receiver_group": receiver_group,
                "receiver": receiver,
                "assigned_partitions": receiver_partition_numbers,
                "status": STATUS.ALL_PARTITIONS_ASSIGNED,
            }

        assigned_partitions = list(receiver_partition_numbers)
        if requested_partition is not None:
            receiver_partition_numbers = (
                [requested_partition]
                if requested_partition in assigned_partitions
                else []
            )

        done = False
        while not done:
            number_of_partitions = len(receiver_partition_numbers)
            if number_of_partitions == 0:
                done = True
                continue

            partition_index = random.randint(0, number_of_partitions - 1)
            partition_number = receiver_partition_numbers.pop(partition_index)

            partition = self._get_partition(stream, partition_number)
            item = partition.get(
                receiver_group, index, skip_pruned=skip_pruned
            )

            if item is None:
                continue

            return {
                "stream": stream,
                "receiver_group": receiver_group,
                "receiver": receiver,
                "partition": partition_number,
                "index": item["index"],
                "value": item["value"],
                "timestamp": item["timestamp"],
                "assigned_partitions": assigned_partitions,
                "status": STATUS.OK,
            }

        return {
            "stream": stream,
            "receiver_group": receiver_group,
            "receiver": receiver,
            "assigned_partitions": assigned_partitions,
            "status": STATUS.END_OF_STREAM,
        }

    @handle_error
    def get_partitions(self, params: dict) -> dict:
        stream = params["stream"]
        receiver_group = params["receiver_group"]
        receiver = params["receiver"]

        self.knock(params, do_log=False)

        receiver_partition_numbers = self._get_receiver_partition_numbers(
            stream,
            receiver_group,
            receiver,
        )

        return {
            "stream": stream,
            "receiver_group": receiver_group,
            "receiver": receiver,
            "assigned_partitions": receiver_partition_numbers,
        }

    @handle_error
    def commit_message(self, params: dict) -> dict:
        stream = params["stream"]
        partition_number = params["partition"]
        index = params["index"]
        receiver_group = params["receiver_group"]

        partition = self._get_partition(stream, partition_number)
        partition.commit(index, receiver_group)

        return {
            "stream": stream,
            "receiver_group": receiver_group,
            "status": STATUS.OK,
        }

    @handle_error
    def set_offset(self, params: dict) -> dict:
        stream = params["stream"]
        partition_number = params["partition"]
        index = params["index"]
        receiver_group = params["receiver_group"]

        partition = self._get_partition(stream, partition_number)
        partition.set_offset(receiver_group, index)

        return {
            "stream": stream,
            "partition": partition_number,
            "index": index,
            "receiver_group": receiver_group,
            "status": STATUS.OK,
        }

    def _get_partition(self, stream: str, partition_number: int, create_if_missing=False):
        utils.validate_partition(partition_number)
        self._get_stream_path(stream)
        with self.partitions_lock:
            if stream not in self.partitions:
                self.partitions[stream] = {}

            if partition_number not in self.partitions[stream]:
                self.partitions[stream][partition_number] = Partition(
                    stream=stream,
                    number=partition_number,
                    data_dir=self.config["global"]["data_dir"],
                    create_if_missing=create_if_missing,
                )
        return self.partitions[stream][partition_number]

    def _get_receiver_partition_numbers(
        self,
        stream,
        receiver_group,
        receiver,
    ):
        with self.partitions_by_group_lock:
            if stream not in self.partitions_by_group:
                self.partitions_by_group[stream] = {}

            if receiver_group not in self.partitions_by_group[stream]:
                self.partitions_by_group[stream][receiver_group] = {}

            if (
                receiver
                not in self.partitions_by_group[stream][receiver_group]
            ):
                self.partitions_by_group[stream][receiver_group][receiver] = []

            return list(
                self.partitions_by_group[stream][receiver_group][receiver]
            )

    def _get_stream_path(self, stream: str) -> str:
        return str(utils.stream_path(
            self.config['global']['data_dir'], stream))

    def _get_stream_partition_numbers(self, stream: str):
        stream_path = self._get_stream_path(stream)
        with self.partitions_by_stream_lock:
            if stream in self.partitions_by_stream:
                return list(self.partitions_by_stream[stream])
            partition_numbers = []

            try:
                partitions_target = self.config["streams"][stream][
                    "partitions"
                ]
            except KeyError:
                partitions_target = self.config["global"]["partitions"]

            if path.isdir(stream_path):
                for partition_number in sorted(listdir(stream_path)):
                    try:
                        partition_numbers.append(int(partition_number))
                    except ValueError:
                        continue

            existing_partitions = len(partition_numbers)
            if partitions_target > existing_partitions:
                for partition_number in range(
                    existing_partitions, partitions_target
                ):
                    if partition_number in partition_numbers:
                        raise FileNotFoundError(
                            f"missing partitions among {partition_numbers}"
                        )

                    self._get_partition(stream, partition_number, create_if_missing=True)
                    partition_numbers.append(partition_number)

            self.partitions_by_stream[stream] = partition_numbers
            return list(partition_numbers)

    def _rebalance_loop(self):
        while True:
            self._rebalance()
            remaining_seconds = self.config["global"]["rebalance_interval"]
            logging.debug(
                f"next rebalance will hapen in {remaining_seconds} seconds"
            )
            time.sleep(self.config["global"]["rebalance_interval"])

    def _rebalance(self):
        with self.partitions_by_group_lock, self.last_seen_by_group_lock:
            logging.debug("rebalancing...")

            receivers_to_remove = []
            for stream in self.partitions_by_group:

                for receiver_group in self.partitions_by_group[stream].keys():
                    stream_receiver_group_receivers = []

                    for receiver in self.partitions_by_group[stream][
                        receiver_group
                    ].keys():
                        receiver_unseen_time = (
                            utils.get_timestamp_ms()
                            - self.last_seen_by_group[receiver_group][receiver]
                        ) / 1000

                        if (
                            receiver_unseen_time
                            < self.config["global"]["receiver_timeout"]
                        ):
                            stream_receiver_group_receivers.append(receiver)

                        else:
                            receivers_to_remove.append(
                                (stream, receiver_group, receiver)
                            )

                    stream_partition_numbers = (
                        self._get_stream_partition_numbers(stream)
                    )

                    number_of_partitions = len(stream_partition_numbers)
                    number_of_receivers = len(stream_receiver_group_receivers)

                    if number_of_receivers > number_of_partitions:
                        number_of_partitions = number_of_receivers

                    if not number_of_receivers:
                        continue

                    step = number_of_partitions // number_of_receivers
                    remainder = number_of_partitions % number_of_receivers

                    for index in range(
                        0, number_of_partitions - remainder, step
                    ):
                        receiver_index = index // step
                        self.partitions_by_group[stream][receiver_group][
                            stream_receiver_group_receivers[receiver_index]
                        ] = stream_partition_numbers[index: index + step]

                    for index in range(
                        number_of_partitions - remainder, number_of_partitions
                    ):
                        receiver_index = index - number_of_partitions + 1
                        self.partitions_by_group[stream][receiver_group][
                            stream_receiver_group_receivers[receiver_index]
                        ].append(stream_partition_numbers[index])

            for stream, receiver_group, receiver in receivers_to_remove:
                logging.info(
                    f'receiver "{receiver}" kicked from the '
                    f'receiver_group "{receiver_group}" '
                    f'for the stream "{stream}"'
                )
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

            if self.partitions_by_group != self._logged_partitions_by_group:
                utils.log_dict(self.partitions_by_group)
                self._logged_partitions_by_group = deepcopy(self.partitions_by_group)

    def _prune_loop(self):
        while True:
            time.sleep(self.config["global"]["prune_interval"])
            streams_path = f"{self.config['global']['data_dir']}/streams/"
            if not path.isdir(streams_path):
                continue
            for stream in listdir(streams_path):
                try:
                    stream_path = self._get_stream_path(stream)
                except ValueError:
                    logging.warning(
                        'Skipping invalid stream directory %r', stream)
                    continue
                if path.isdir(stream_path):
                    partition_numbers = []
                    for partition_number in sorted(listdir(stream_path)):
                        try:
                            partition_numbers.append(int(partition_number))
                        except ValueError:
                            continue

                    stream_with_defined_ttl = (
                        "streams" in self.config
                        and stream in self.config["streams"]
                        and "ttl" in self.config["streams"][stream]
                    )

                    if stream_with_defined_ttl:
                        ttl = self.config["streams"][stream]["ttl"]
                    else:
                        ttl = self.config["global"]["ttl"]

                    for partition_number in partition_numbers:
                        logging.info(
                            f"pruning stream {stream} (partition"
                            f" {partition_number})"
                        )

                        try:
                            partition = self._get_partition(
                                stream, partition_number)
                        except ValueError:
                            logging.warning('Skipping invalid partition %r/%r',
                                            stream, partition_number)
                            continue
                        partition.prune(int(ttl))
