#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.venv/bin/activate"

# Helper to supress informational output from the command
run_quiet() {
    local command_name="$1"
    shift

    local log_file
    log_file="$(mktemp)"

    if "$@" >/dev/null 2>"$log_file"; then
        rm -f "$log_file"
        return 0
    fi

    echo "ERROR: ${command_name} failed."
    echo "------- ${command_name} logs -------"
    cat "$log_file"
    echo "----- end ${command_name} logs -----"
    rm -f "$log_file"
    return 1
}

# Python used by the driver (runs locally on the master node)
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

echo ""
echo "> Preparing data for indexing"

run_quiet "Upload parquet source" hdfs dfs -put -f a.parquet /
run_quiet "Cleanup /data" hdfs dfs -rm -r -f /data
run_quiet "Cleanup /input/data" hdfs dfs -rm -r -f /input/data
run_quiet "Prepare data Spark job" spark-submit --driver-memory 2g prepare_data.py

echo ""
echo "> Data is ready for indexing"