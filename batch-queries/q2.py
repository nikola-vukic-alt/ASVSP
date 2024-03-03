from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_2"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

negative_threshold = 0.4  # 40%

df_rotten_reviews = df_reviews.filter(df_reviews["scoreSentiment"] == "NEGATIVE")

# Kojih 10 žanrova ima najviše “rotten” filmova?
# Za film se smatra da je "rotten" ukoliko ima vise od 40% negativnih utisaka
df = df_movies.join(df_rotten_reviews, "movie_id") \
    .withColumn("genre", explode(split(regexp_replace("genre", "\s*,\s*", ","), ","))) \
    .groupBy("genre", "movie_id") \
    .agg(F.countDistinct("movie_id").alias("rotten_movies")) \
    .groupBy("genre") \
    .agg(F.sum("rotten_movies").alias("total_rotten_movies")) \
    .filter((F.col("total_rotten_movies") / F.countDistinct("movie_id")) > negative_threshold) \
    .orderBy(F.desc("total_rotten_movies")) \
    .limit(10)

print("QUERY: Kojih 10 žanrova ima najviše “rotten” filmova?\n")
df.show()

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

df.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("overwrite") \
    .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
    .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
    .option("mergeSchema", "true") \
    .option('es.index.auto.create', 'true') \
    .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
    .option('es.port', ELASTIC_SEARCH_PORT) \
    .option('es.batch.write.retry.wait', '10s') \
    .save(ELASTIC_SEARCH_INDEX)

current_date = date.today().strftime("%Y/%m/%d")
current_time = datetime.now().strftime("%H:%M:%S")
print(f"{current_date[2:]} {current_time} INFO Added new index: '{ELASTIC_SEARCH_INDEX}'.")
