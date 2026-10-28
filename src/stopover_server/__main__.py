#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .version import __version__
from .broker import Broker
import logging
import yaml
import sys
from .server import create_server

banner = f"""
                                      █████████
    █████████                       █████████████
  █████████████                   █████████████████
 ███████████████                 ███████████████████
  █████████████                   █████████████████
    █████████  ███                  █████████████
                  ███                 █████████
                     ███                 ███
                █████████████          ███
              █████████████████      ███
            █████████████████████ ███
          █████████████████████████
         ███████████████████████████
          █████████████████████████
            █████████████████████
              █████████████████
                █████████████
                                 Stopover v{__version__}
"""

CONFIG_PATH = './config.yaml'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format='%(asctime)-15s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main():
    logging.info(f'\n{banner}')

    with open(CONFIG_PATH, 'r') as input_file:
        config = yaml.safe_load(input_file)

    try:
        file = open(f"{config['global']['data_dir']}/streams/.active")
        file.close()
    except FileNotFoundError:
        logging.critical('the streams dir is not active')
        sys.exit(1)

    port = config['global']['port'] if 'port' in config['global'] else 5704
    server = create_server(Broker(config), host='0.0.0.0', port=int(port),
                           threads=config['global'].get('threads', 4))
    try:
        server.print_listen('Serving on http://{}:{}')
        server.run()
    finally:
        server.task_dispatcher.shutdown()
        server.close()


if __name__ == "__main__":
    main()
