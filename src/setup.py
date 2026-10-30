#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages
from pathlib import Path
import re

ROOT = Path(__file__).parent
VERSION = re.search(r"^__version__ = '([^']+)'",
                    (ROOT / 'stopover_server/version.py').read_text(), re.M)[1]

setup(name='stopover-server',
      version=VERSION,
      long_description='A simple and robust message broker built on top of RocksDB.',
      long_description_content_type='text/plain',
      description='A simple and robust message broker built on top of RocksDB',
      url='https://github.com/labteral/stopover',
      author='Rodrigo Martínez Castaño',
      author_email='dev@brunneis.com',
      license='GNU General Public License v3 (GPLv3)',
      packages=find_packages(),
      zip_safe=False,
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3.14',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      python_requires='>=3.10,<4',
      install_requires=[
          'msgpack >= 1.2.4, < 2.0.0',
          'python-snappy >= 0.7.3, < 1.0.0',
          'cramjam >= 2.14.0, < 3.0.0',
          'easyrocks >= 4.2610.0, < 5.0.0',
          'pyyaml >= 6.0.3, < 7.0.0',
          'falcon >= 4.4.0, < 5.0.0',
          'waitress >= 3.0.2, < 4.0.0',
      ],
      entry_points={
          'console_scripts': [
              'stopover = stopover_server.__main__:main',
          ],
      })
