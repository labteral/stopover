#!/bin/bash
export STOPOVER_VERSION=$(cat ../src/stopover_server/version.py | grep version | cut -f2 -d\' | head -1 | xargs)
