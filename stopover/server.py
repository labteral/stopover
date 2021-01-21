#!/usr/bin/env python
# -*- coding: utf-8 -*-

from stopover import banner
from broker import Broker
import falcon
import logging
import yaml
import sys
from cherrybone import Server
from os import environ

CONFIG_PATH = './config.yaml'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.info(f'\n{banner}')

with open(CONFIG_PATH, 'r') as input_file:
    config = yaml.safe_load(input_file)

try:
    open(f"{config['global']['data_dir']}/streams/.active")

except FileNotFoundError:
    logging.critical('the streams dir is not active')
    sys.exit(1)

api = falcon.App()
api.add_route('/', Broker(config))

if __name__ == "__main__":
    threads = None
    if config['threads'] > 0:
        threads = config['threads']

    Server(api, port=5704, threads=threads).start()