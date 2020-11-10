#!/bin/bash
source env.sh
cd ../stopover && tar --dereference -c -f ../docker/stopover.tar * && cd ../docker
docker build -t labteral/stopover-server:latest --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION .
docker tag labteral/stopover-server labteral/stopover-server:$STOPOVER_VERSION
rm -f stopover.tar
