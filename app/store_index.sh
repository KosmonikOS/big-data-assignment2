#!/bin/bash

set -e

HDFS_BASE=${1:-/indexer}

source /app/.venv/bin/activate

python3 /app/load_index_to_cassandra.py \
    --hdfs-base "$HDFS_BASE" \
    --cassandra-host "cassandra-server"