#!/bin/bash

INPUT_PATH=${1:-/input/data}
MAPREDUCE_DIR=/app/mapreduce

# Locate the Hadoop Streaming JAR bundled with the Hadoop installation
HADOOP_STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" 2>/dev/null | head -1)

if [ -z "$HADOOP_STREAMING_JAR" ]; then
    echo "ERROR: Could not find hadoop-streaming JAR under $HADOOP_HOME"
    exit 1
fi

echo "============================================================"
echo "  Hadoop Streaming JAR : $HADOOP_STREAMING_JAR"
echo "  Input path           : $INPUT_PATH"
echo "============================================================"

# Pipeline 1: Inverted Index (TF + DF)
echo ""
echo ">>> Pipeline 1: Building inverted index (TF + DF) ..."
hdfs dfs -rm -r -f /indexer/index

hadoop jar "$HADOOP_STREAMING_JAR" \
    -files "$MAPREDUCE_DIR/mapper1.py","$MAPREDUCE_DIR/reducer1.py" \
    -input  "$INPUT_PATH" \
    -output /indexer/index \
    -mapper  "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -numReduceTasks 1

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 1 failed."
    exit 1
fi
echo ">>> Pipeline 1 complete. Index written to /indexer/index"

# Pipeline 2: Vocabulary
echo ""
echo ">>> Pipeline 2: Extracting vocabulary ..."
hdfs dfs -rm -r -f /indexer/vocabulary

hadoop jar "$HADOOP_STREAMING_JAR" \
    -files "$MAPREDUCE_DIR/mapper2.py","$MAPREDUCE_DIR/reducer2.py" \
    -input  /indexer/index \
    -output /indexer/vocabulary \
    -mapper  "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -numReduceTasks 1

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 2 failed."
    exit 1
fi
echo ">>> Pipeline 2 complete. Vocabulary written to /indexer/vocabulary"

# Pipeline 3: Document Statistics
echo ""
echo ">>> Pipeline 3: Computing document statistics (lengths + corpus totals) ..."
hdfs dfs -rm -r -f /indexer/stats

hadoop jar "$HADOOP_STREAMING_JAR" \
    -files "$MAPREDUCE_DIR/mapper3.py","$MAPREDUCE_DIR/reducer3.py" \
    -input  "$INPUT_PATH" \
    -output /indexer/stats \
    -mapper  "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -numReduceTasks 1

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline 3 failed."
    exit 1
fi
echo ">>> Pipeline 3 complete. Statistics written to /indexer/stats"

echo ""
echo "============================================================"
echo "  Index creation complete."
echo "  /indexer/index      — inverted index (term, doc_id, tf, df)"
echo "  /indexer/vocabulary — vocabulary    (term, df)"
echo "  /indexer/stats      — doc stats     (doc_id, length) + global N, dlavg"
echo "============================================================"
