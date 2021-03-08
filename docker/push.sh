#!/bin/bash
source env.sh
docker push labteral/stopover-server:$STOPOVER_VERSION
docker push labteral/stopover-server:latest

