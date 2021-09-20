#!/bin/bash
cd $(dirname $0)
source env.sh
docker pull labteral/stopover-server
docker pull labteral/stopover-server:$STOPOVER_VERSION
