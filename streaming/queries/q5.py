from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, ArrayType, FloatType
from os import environ

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/streaming/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

ELASTIC_SEARCH_INDEX = "streaming_query_5"

def save_data(df, ELASTIC_SEARCH_INDEX):
    df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    df \
        .writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/EL_" + ELASTIC_SEARCH_INDEX) \
        .format('org.elasticsearch.spark.sql') \
        .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
        .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
        .option("mergeSchema", "true") \
        .option('es.index.auto.create', 'true') \
        .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
        .option('es.port', ELASTIC_SEARCH_PORT) \
        .option('es.batch.write.retry.wait', '100s') \
        .start(ELASTIC_SEARCH_INDEX)
    
    df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", OUTPUT_PATH + ELASTIC_SEARCH_INDEX) \
        .option("checkpointLocation", "/tmp/" + ELASTIC_SEARCH_INDEX) \
        .start()
        
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Reviewed Genres") \
    .getOrCreate()

quiet_logs(spark)

# Define a schema for the reviews data
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("imdbId", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)  
])

# Load the streaming data from Kafka
reviews = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "reviews-topic") \
    .load()

# Convert the value column from Kafka into a string
reviews = reviews.withColumn("value", col("value").cast("string"))

# Parse the JSON data from the value column
reviews = reviews.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"

df_movies = spark.read.csv(MOVIES_PATH, header=True, inferSchema=True)

# Ocjena filma na rotten tomatoes od strane publike (batch, normalizovana na 1-5 opseg) vs 
# ocjena filma od strane publike na IMDB (streaming u prethodnih 10 minuta). Azurirano svakih 30 sekundi.
review_ratings = reviews \
    .join(df_movies, reviews.imdbId == df_movies.imdb_id, "left") \
    .select(
        window(col("timestamp"), "10 minutes").alias("window"),
        col("title"),
        (col("audienceScore") / 20.0).alias("rotten_tomatoes_rating"),
        col("rating").cast("float").alias("imdb_rating")
    ) \
    .na.drop() \
    .withWatermark("window", "10 minutes") \
    .groupBy("window", "title") \
    .agg(
        round(avg("rotten_tomatoes_rating"), 2).alias("avg_rotten_tomatoes_rating"),
        round(avg("imdb_rating"), 2).alias("avg_imdb_rating")
    ) 

save_data(review_ratings, ELASTIC_SEARCH_INDEX)
spark.streams.awaitAnyTermination()
