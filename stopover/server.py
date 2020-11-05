#!/usr/bin/env python
# -*- coding: utf-8 -*-

import falcon
import logging
import yaml
import sys
from broker import Broker
from gunicorn.app.base import BaseApplication

banner = """
  ███████████                             ███████████
 █████████████                          ███████████████
███████████████                        █████████████████
 █████████████                        ███████████████████
  ███████████ ███                      █████████████████
                 ███                    ███████████████
                    ███                   ███████████
                      ███                     ███
                       ███                   ███
                        ███                 ███
                █████████████████          ███
             ███████████████████████     ███
            █████████████████████████ ███
          █████████████████████████████
          █████████████████████████████
          █████████████████████████████
          █████████████████████████████
           ███████████████████████████
             ███████████████████████
               ███████████████████
                   ███████████        ___________________
                                            Stopover v0.1
"""

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
    sys.exit(4)

api = falcon.App()
api.add_route('/', Broker(config))


class Server(BaseApplication):
    def __init__(self, app, options=None):
        self.app = app
        self.options = options or {}
        super(Server, self).__init__()

    def load_config(self):
        for key, value in self.options.items():
            self.cfg.set(key, value)

    def load(self):
        return self.app

    def run(self):
        super().run()


if __name__ == "__main__":
    options = {'bind': f"0.0.0.0:5704", 'workers': 1, 'threads': 16, 'keepalive': 300, 'timeout': 0}
    Server(api, options).run()