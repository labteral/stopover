#!/bin/bash
cd docker
source env.sh
tar --dereference -c -f stopover.tar ../*.py
docker build -t labteral/stopover-server:latest --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION .
docker tag labteral/stopover-server labteral/stopover-server:$STOPOVER_VERSION
rm -f stopover.tar
cd ..
