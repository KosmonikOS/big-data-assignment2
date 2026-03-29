#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper to suppress informational output from the command
run_quiet() {
    local command_name="$1"
    shift

    local log_file
    log_file="$(mktemp)"

    if "$@" 2>"$log_file"; then
        rm -f "$log_file"
        return 0
    fi

    echo "ERROR: ${command_name} failed." >&2
    echo "------- ${command_name} logs -------" >&2
    cat "$log_file" >&2
    echo "----- end ${command_name} logs -----" >&2
    rm -f "$log_file"
    return 1
}

# Python used by the driver (runs locally on the master node)
export PYSPARK_DRIVER_PYTHON=$(which python)
# Python used by executors — points at the venv unpacked from the archive
export PYSPARK_PYTHON=./.venv/bin/python

echo ""
echo ">>> Loading HDFS index into Cassandra"

run_quiet "Load Index Spark job" spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    "$SCRIPT_DIR/load_index_to_cassandra.py"
