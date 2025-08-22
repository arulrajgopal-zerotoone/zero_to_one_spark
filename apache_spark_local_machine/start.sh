#!/bin/bash


# Check if a script file is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <pyspark_script.py>"
    exit 1
fi

PYSPARK_SCRIPT="$1"

# Directory to store Spark event logs
EVENT_LOG_DIR="/home/Arulraj/Desktop/spark_events/"

# Make sure the directory exists
mkdir -p "$EVENT_LOG_DIR"

# Check if Spark History Server is running
if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
    echo "Spark History Server is already running."
else
    echo "Starting Spark History Server..."
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$EVENT_LOG_DIR" \
    "$SPARK_HOME/sbin/start-history-server.sh"
fi

# Run the PySpark job with event logging
# spark-submit \
#   --conf spark.eventLog.enabled=true \
#   --conf spark.eventLog.dir="file://$EVENT_LOG_DIR" \
#   --packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
#   "$PYSPARK_SCRIPT"


spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="file://$EVENT_LOG_DIR" \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  --py-files utils.py \
  "$PYSPARK_SCRIPT"
