#!/bin/bash
BATCH_DATANODE_PATH="/asvsp/raw/batch"

hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/movies"
echo "Created movies directory."

hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/reviews"
echo "Created reviews directory."