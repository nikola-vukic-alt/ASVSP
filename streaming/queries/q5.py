from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
from os import environ

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/streaming/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

ELASTIC_SEARCH_INDEX = "streaming_query_5"

def save_data(df, ELASTIC_SEARCH_INDEX):
    df \
        .writeStream \
        .outputMode("update") \
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

schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("imdbId", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("title", StringType(), True)  
])

reviews = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "reviews-topic") \
    .load()

reviews = reviews.withColumn("value", col("value").cast("string"))

reviews = reviews.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"

df_movies = spark.read.csv(MOVIES_PATH, header=True, inferSchema=True)

# Ocjena filma na rotten tomatoes od strane publike (batch, normalizovana na 1-5 opseg) vs 
# ocjena filma od strane publike na IMDB (streaming u prethodnih 10 minuta). Azurirano svakih 30 sekundi.
review_ratings = reviews \
    .join(df_movies, reviews.imdbId == df_movies.imdb_id, "left") \
    .select(
        window(col("timestamp"), "2 minutes").alias("window"),
        reviews["title"],
        (col("audienceScore") / 20.0).alias("rotten_tomatoes_rating"),
        col("rating").cast("float").alias("imdb_rating")
    ) \
    .na.drop() \
    .withWatermark("window", "2 minutes") \
    .groupBy("window", reviews["title"]) \
    .agg(
        round(avg("rotten_tomatoes_rating"), 2).alias("avg_rotten_tomatoes_rating"),
        round(avg("imdb_rating"), 2).alias("avg_imdb_rating")
    )  

elasticsearch_schema = StructType([
    StructField("window", StructType([
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True)
    ])),
    StructField("title", StringType(), True),
    StructField("avg_rotten_tomatoes_rating", FloatType(), True),
    StructField("avg_imdb_rating", FloatType(), True)
])

review_ratings_with_schema = review_ratings.select(
    col("window").cast(elasticsearch_schema["window"].dataType).alias("window"),
    col("title"),
    col("avg_rotten_tomatoes_rating"),
    col("avg_imdb_rating")
)

review_ratings_with_schema.printSchema()

save_data(review_ratings_with_schema, ELASTIC_SEARCH_INDEX)
spark.streams.awaitAnyTermination()
