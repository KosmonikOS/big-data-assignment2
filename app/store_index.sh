#!/bin/bash
set -e

source /app/.venv/bin/activate

echo ""
echo ">>> Loading HDFS index into Cassandra"

python3 /app/load_index_to_cassandra.py