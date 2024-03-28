#!/bin/bash
BATCH_DATANODE_PATH="/asvsp/raw"

hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/movies"
echo "Created Rotten Tomatoes Movies directory."
hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/reviews"
echo "Created Rotten Tomatoes Reviews directory."
hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/movies_imdb"
echo "Created IMDB Movies directory"
hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/mapper"
echo "Created mapper directory"
hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/links"
echo "Created links directory"

hdfs dfs -mkdir -p "/streaming/sink"
echo "Created streaming data sink directory."



