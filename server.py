#!/usr/bin/env python
# -*- coding: utf-8 -*-

import falcon
import logging
import yaml
import sys
from broker import Broker

banner = """\033[94m
███████ ████████ ████████ ███████ ████████ ██    ██ ███████ ██████  
██         ██    ██    ██ ██   ██ ██    ██ ██    ██ ██      ██   ██ 
███████    ██    ██    ██ ███████ ██    ██ ██    ██ █████   ██████  
     ██    ██    ██    ██ ██      ██    ██  ██  ██  ██      ██   ██ 
███████    ██    ████████ ██      ████████   ████   ███████ ██   ██\033[93m
                                                Stopover v0.1-alpha\033[0m
"""
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.info(f'\n{banner}')

CONFIG_PATH = './config.yaml'

with open(CONFIG_PATH, 'r') as input_file:
    config = yaml.safe_load(input_file)

try:
    open(f"{config['global']['data_dir']}/streams/.active")
except FileNotFoundError:
    logging.critical('the streams dir is not active')
    sys.exit(4)

api = falcon.App()
api.add_route('/', Broker(config))
