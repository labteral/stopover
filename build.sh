#!/bin/bash
source env.sh
docker build -t labteral/stopover-server:$STOPOVER_VERSION --build-arg ROCKSDB_VERSION=$ROCKSDB_VERSION -f docker/Dockerfile .
docker tag labteral/stopover-server:$STOPOVER_VERSION labteral/stopover-server:latest

