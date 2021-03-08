#!/bin/bash
source env.sh
cd ../stopover && tar --dereference -c -f ../docker/stopover.tar * && cd ../docker
docker build -t labteral/stopover-server:$STOPOVER_VERSION --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION .
docker tag labteral/stopover-server:$STOPOVER_VERSION labteral/stopover-server:latest
rm -f stopover.tar
