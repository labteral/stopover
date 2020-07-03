#!/bin/bash
source env.sh
tar --dereference -c -f stopover.tar *.py
docker build -t labteral/stopover:latest --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION .
docker tag labteral/stopover labteral/stopover:$STOPOVER_VERSION
rm -f stopover.tar
