#!/bin/bash
# set -e

cd $(dirname "$0")
mkdir -p logs
DATE=$(date "+%Y_%m_%d")

export RES_PATH=res/actions
mkdir -p $RES_PATH

export ACTION=actions
export PORT=3015
export WS_PORT=3016
export HISTORY_LIMIT=20000

yarn start 2>&1 | tee -a logs/actions_$DATE.txt
