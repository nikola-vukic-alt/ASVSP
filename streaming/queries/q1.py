from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from os import environ

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/streaming/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

ELASTIC_SEARCH_INDEX = "streaming_query_1"

def save_data(df, ELASTIC_SEARCH_INDEX):
    df \
        .writeStream \
        .outputMode("complete") \
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
        .option('es.nodes', 'http://{}'.format(ELASTIC_SEARCH_NODE)) \
        .option('es.port', ELASTIC_SEARCH_PORT) \
        .option('es.batch.write.retry.wait', '100s') \
        .start(ELASTIC_SEARCH_INDEX)
    
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Top Rated Movies") \
    .getOrCreate()

quiet_logs(spark)

# Define a schema for the reviews data
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("rating", DoubleType(), True),
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
reviews = reviews.select(from_json(col("value"), schema).alias("review")).select("review.*")

# Koji su najviše kritikovani filmovi u prethodnih minut? Azurirano svakih 30 sekundi.
review_counts = reviews \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "30 seconds"), "movieId", "title") \
    .count()

top_movies = review_counts \
    .limit(20)  

save_data(top_movies, ELASTIC_SEARCH_INDEX)

spark.streams.awaitAnyTermination()
