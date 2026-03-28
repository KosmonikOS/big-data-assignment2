#!/bin/bash
set -e

QUERY="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$SCRIPT_DIR/.venv/bin/activate"

# Python used by the driver (runs locally on the master node)
export PYSPARK_DRIVER_PYTHON=$(which python)
# Python used by executors — points at the venv unpacked from the archive
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    "$SCRIPT_DIR/query.py" \
    "$QUERY"
