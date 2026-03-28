#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_PATH="/input/data"

echo ""
echo ">>> [1/2] Running MapReduce pipelines"
bash "$SCRIPT_DIR/create_index.sh" "$INPUT_PATH"

echo ""
echo ">>> [2/2] Loading HDFS index into Cassandra"
bash "$SCRIPT_DIR/store_index.sh"