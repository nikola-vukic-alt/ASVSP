#!/bin/bash
BATCH_DATANODE_PATH="/asvsp/raw/batch"
MAPPER_DATANODE_PATH="/asvsp/mapper"

hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/movies"
echo "Created movies directory."

# HDFS_MOVIES_DIR="$DATANODE_PATH/movies/"
# hdfs dfs -put "$LOCAL_CSV_MOVIES_FILE" "$HDFS_MOVIES_DIR"
# echo "Copied $LOCAL_CSV_MOVIES_FILE to $HDFS_MOVIES_DIR"

hdfs dfs -mkdir -p "$BATCH_DATANODE_PATH/reviews"
echo "Created reviews directory."

# HDFS_REVIEWS_DIR="$DATANODE_PATH/reviews/"
# hdfs dfs -put "$LOCAL_CSV_REVIEWS_FILE" "$HDFS_REVIEWS_DIR"
# echo "Copied $LOCAL_CSV_REVIEWS_FILE to $HDFS_REVIEWS_DIR"

hdfs dfs -mkdir -p "$MAPPER_DATANODE_PATH/"
echo "Created mapper directory."

# HDFS_MAPPER_DIR="$DATANODE_PATH/mapper/"
# hdfs dfs -put "$LOCAL_CSV_MAPPER_FILE" "$HDFS_MAPPER_DIR"
# echo "Copied $LOCAL_CSV_MAPPER_FILE to $HDFS_MAPPER_DIR"
# echo
# echo "Checking the existence of files..."
# echo "Movies:"
# hdfs dfs -ls /asvsp/raw/batch/movies/
# echo "Reviews:"
# hdfs dfs -ls /asvsp/raw/batch/reviews/
# echo "Mapper:"
# hdfs dfs -ls /asvsp/raw/batch/mapper/

# hdfs dfs -chmod 755 /asvsp/raw/batch/movies/raw_rotten_tomatoes_movies.csv
# hdfs dfs -chmod 755 /asvsp/raw/batch/reviews/raw_rotten_tomatoes_movie_reviews.csv
# hdfs dfs -chmod 755 /asvsp/raw/batch/mapper/raw_movie_mapper.csv
