from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from os import environ

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/streaming/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

ELASTIC_SEARCH_INDEX = "streaming_query_2"

def save_data(df, ELASTIC_SEARCH_INDEX):
    df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    def generate_doc_id(genre, language):
        return concat_ws("_", genre, language)

    df_with_doc_id = df.withColumn("doc_id", generate_doc_id(col("genre"), col("language")))

    df_with_doc_id \
        .writeStream \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/EL_" + ELASTIC_SEARCH_INDEX) \
        .format('org.elasticsearch.spark.sql') \
        .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
        .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
        .option("mergeSchema", "true") \
        .option('es.index.auto.create', 'true') \
        .option('es.nodes', 'http://{}'.format(ELASTIC_SEARCH_NODE)) \
        .option('es.port', ELASTIC_SEARCH_PORT) \
        .option('es.batch.write.retry.wait', '100s') \
        .option("es.mapping.id", "doc_id") \
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
    StructField("title", StringType(), True),
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

# Split the genres field and explode it into separate genre entries

MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"

df_movies = spark.read.csv(MOVIES_PATH, header=True, inferSchema=True)

# df_movies = df_movies.withColumn("genre", explode(split(trim(col("genre")), "\\|")))

review_ratings = reviews \
    .join(df_movies, reviews.imdbId == df_movies.imdb_id, "left") \
    .withColumn("language", when(col("originalLanguage") == "English", "English").otherwise("Non-English")) \
    .select(
        window(col("timestamp"), "3 minutes").alias("window"),
        explode(split(col("genre"), ",")).alias("genre"),  
        col("language"),
        col("rating")
    ) \
    .withColumn("genre", trim(col("genre"))) \
    .na.drop() \
    .withWatermark("window", "3 minutes") \
    .groupBy("window", "genre", "language") \
    .agg(avg("rating").alias("rating")) \
    .withWatermark("window", "3 minutes") 

save_data(review_ratings, ELASTIC_SEARCH_INDEX)

spark.streams.awaitAnyTermination()
