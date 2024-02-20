#!/bin/bash
# set -e

cd $(dirname "$0")
mkdir -p logs
DATE=$(date "+%Y_%m_%d")

export RES_PATH=res/events
mkdir -p $RES_PATH

export DATABASE_TABLE=events
export ACTION=events
export PORT=3005
export WS_PORT=3006


yarn start 2>&1 | tee -a logs/events_$DATE.txt
