#!/bin/bash
cd $(dirname $0)
source env.sh
docker push labteral/stopover-server:$STOPOVER_VERSION
docker push labteral/stopover-server:latest

