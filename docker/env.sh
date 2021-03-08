#!/bin/bash
export STOPOVER_VERSION=$(cat ../stopover/stopover.py | grep version | cut -f2 -d\' | head -1 | xargs)
export ROCKSDB_VERSION=6.10.2
