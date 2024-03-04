from os import environ
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import kafka.errors
import time 

KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "localhost:29092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(environ.get("KAFKA_RECONNECT_BACKOFF_MS", "100"))
}

def get_connection():
    while True:
        try:
            producer = KafkaProducer(**KAFKA_CONFIGURATION)
            print("Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(1)

def producing():
    producer = get_connection()
    spark = SparkSession.builder.appName("Streaming Reviews").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df_reviews = spark.read.csv(path="/data/streaming/reviews.csv", header=True, inferSchema=True)

    for row in df_reviews.collect():
        key = f"{row['userId']}_{row['movieId']}_{row['timestamp']}"
        
        producer.send("reviews-topic", key=str(key), value=row.asDict())
        print("Sent row to Kafka:", row)
        time.sleep(1)

if __name__ == "__main__":
    producing()