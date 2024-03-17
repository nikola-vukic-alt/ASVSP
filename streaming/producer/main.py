from os import environ
from pyspark.sql import SparkSession, Row
from datetime import datetime
from kafka import KafkaProducer
import kafka.errors
import time
from json import dumps

KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "localhost:29092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(environ.get("KAFKA_RECONNECT_BACKOFF_MS", "100"))
}

spark = SparkSession.builder.appName("Streaming Reviews to Kafka").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
df_movies = spark.read.csv(MOVIES_PATH, header=True, inferSchema=True)
# Collect the DataFrame as a list of dictionaries
movies_list = df_movies.collect()

# Convert the list of dictionaries to a dictionary
movies_dict = {row["imdb_id"]: row for row in movies_list}

def send_partition_to_kafka(partition):
    producer = KafkaProducer(**KAFKA_CONFIGURATION)
    for row in partition:
        # Convert Row to dictionary
        row_dict = row.asDict()
        if row_dict["imdbId"] not in movies_dict: 
            continue
        
        # Format current time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Add current time to the row dictionary
        row_dict["timestamp"] = current_time
        # Convert back to Row object
        updated_row = Row(**row_dict)
        
        key = f"{updated_row['userId']}_{updated_row['movieId']}_{updated_row['timestamp']}"
        value = updated_row.asDict()
        
        print(f"Sending to Kafka - Key: {key}, Value: {value}")
        
        producer.send("reviews-topic", key=key, value=value)
        time.sleep(1)

    producer.flush()
    producer.close()


def main():
    df_reviews = spark.read.csv("/data/streaming/reviews.csv", header=True, inferSchema=True)
    df_reviews.foreachPartition(send_partition_to_kafka)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
