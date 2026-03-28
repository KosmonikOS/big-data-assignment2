#!/bin/bash
set -e

source .venv/bin/activate

# Python used by the driver (runs locally on the master node)
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Preparing data for indexing" && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data
