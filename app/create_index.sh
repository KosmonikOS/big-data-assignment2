#!/bin/bash

INPUT_PATH=${1:-/input/data}
MAPREDUCE_DIR=/app/mapreduce

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

# Locate the Hadoop Streaming runtime JAR bundled with Hadoop.
HADOOP_STREAMING_JAR=$(
  find "$HADOOP_HOME" -type f -path "*/tools/lib/hadoop-streaming*.jar" 2>/dev/null | sort | head -1
)
if [ -z "$HADOOP_STREAMING_JAR" ]; then
    echo "ERROR: Could not find hadoop-streaming JAR under $HADOOP_HOME"
    exit 1
fi

echo ""
echo ">>> Pipeline 1: Building inverted index"
run_quiet "Cleanup /indexer/index" hdfs dfs -rm -r -f /indexer/index
if [ $? -ne 0 ]; then exit 1; fi

run_quiet "Pipeline 1" hadoop jar "$HADOOP_STREAMING_JAR" \
    -file "$MAPREDUCE_DIR/mapper1.py" \
    -file "$MAPREDUCE_DIR/reducer1.py" \
    -input  "$INPUT_PATH" \
    -output /indexer/index \
    -mapper  "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -numReduceTasks 1

if [ $? -ne 0 ]; then
    exit 1
fi

echo ""
echo ">>> Pipeline 2: Computing document statistics"
run_quiet "Cleanup /indexer/stats" hdfs dfs -rm -r -f /indexer/stats
if [ $? -ne 0 ]; then exit 1; fi

run_quiet "Pipeline 2" hadoop jar "$HADOOP_STREAMING_JAR" \
    -file "$MAPREDUCE_DIR/mapper2.py" \
    -file "$MAPREDUCE_DIR/reducer2.py" \
    -input  "$INPUT_PATH" \
    -output /indexer/stats \
    -mapper  "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -numReduceTasks 1

if [ $? -ne 0 ]; then
    exit 1
fi