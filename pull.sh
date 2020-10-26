#!/bin/bash
source env.sh
docker pull labteral/stopover-server
docker pull labteral/stopover-server:$STOPOVER_VERSION
