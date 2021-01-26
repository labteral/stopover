#!/bin/bash
source env.sh
cd ../stopover && tar --dereference -c -f ../docker/stopover.tar * && cd ../docker
docker build -t labteral/stopover-server:$STOPOVER_VERSION --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION .
rm -f stopover.tar
