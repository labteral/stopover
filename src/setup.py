#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages
import stopover_server

setup(name='stopover-server',
      version=stopover_server.__version__,
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
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      python_requires='>=3.6',
      install_requires=[
          'msgpack >= 1.0.2, < 2.0.0',
          'python-snappy >= 0.5.4, < 1.0.0',
          'easyrocks >= 2.214.0.1, < 3.0.0',
          'pyyaml >= 5.4.1, < 6.0.0',
          'falcon >= 3.0.0, < 4.0.0',
          'bjoern >= 3.1.0, < 4.0.0',
      ],
      entry_points={
          'console_scripts': [
              'stopover = stopover_server.__main__:main',
          ],
      })
