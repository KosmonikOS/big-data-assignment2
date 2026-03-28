#!/bin/bash
set -e

echo ""
echo "> Restarting Services"

# Stop existing services (ignore errors when daemons are not running)
mapred --daemon stop historyserver 2>/dev/null || true
$HADOOP_HOME/sbin/stop-yarn.sh 2>/dev/null || true
$HADOOP_HOME/sbin/stop-dfs.sh  2>/dev/null || true

# Allow ports and PID files to release before restarting
sleep 3

# Start HDFS, YARN, and MapReduce history server
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver

# Leave safemode if the namenode entered it
hdfs dfsadmin -safemode leave

# Create Spark jars directory and user home
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars

# Copy Spark jars to HDFS
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod +rx /apps/spark/jars/

# Create home directory for root user on HDFS
hdfs dfs -mkdir -p /user/root

echo "> All services are ready"