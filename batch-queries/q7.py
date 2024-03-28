from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_7"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

df_genres = df_movies.withColumn("genre", explode(split(col("genre"), ",")))

df_total_reviews = df_genres.groupBy("genre").agg(F.count("*").alias("total_reviews"))

df_genre_scores = df_genres \
    .join(df_total_reviews, "genre", "inner") \
    .filter(col("total_reviews") >= 500) \
    .dropna(subset=["audienceScore", "tomatoMeter"]) \
    .groupBy("genre") \
    .agg(F.avg("audienceScore").alias("avg_audience_score"), F.avg("tomatoMeter").alias("avg_tomato_meter"))

df_genre_scores = df_genre_scores.withColumn("combined_score", (col("avg_audience_score") + col("avg_tomato_meter")) / 2)

top_5_genres = df_genre_scores.orderBy(F.desc("combined_score")).limit(5)

print("QUERY: Koji su 5 najbolje ocijenjenih filmski zanrova po mišljenju publike i po misljenju kriticara?\n")
top_5_genres.show()

# Write the result to JSON and Elasticsearch
top_5_genres.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

top_5_genres.write \
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
