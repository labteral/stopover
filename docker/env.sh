#!/bin/bash
cd $(dirname $0)
export STOPOVER_VERSION=$(cat ../src/stopover_server/version.py | grep version | cut -f2 -d\' | head -1 | xargs)
export ROCKSDB_VERSION=6.10.2
