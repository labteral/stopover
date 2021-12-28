#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .version import __version__
from .broker import Broker
import falcon
import logging
import yaml
import sys
import bjoern

banner = f"""
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
                   ███████████
                                 Stopover v{__version__}
"""

CONFIG_PATH = './config.yaml'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def main():
    logging.info(f'\n{banner}')

    with open(CONFIG_PATH, 'r') as input_file:
        config = yaml.safe_load(input_file)

    try:
        open(f"{config['global']['data_dir']}/streams/.active")

    except FileNotFoundError:
        logging.critical('the streams dir is not active')
        sys.exit(1)

    api = falcon.App(cors_enable=True)
    api.add_route('/', Broker(config))

    port = config['global']['port'] if 'port' in config['global'] else 5704
    bjoern.run(api, '0.0.0.0', port)


if __name__ == "__main__":
    main()
