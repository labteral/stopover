#!/bin/bash
source env.sh
docker push labteral/stopover-server
docker push labteral/stopover-server:$STOPOVER_VERSION
