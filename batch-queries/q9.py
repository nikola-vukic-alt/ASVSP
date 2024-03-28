from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_9"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

df_movies = df_movies.withColumn("writer", explode(split(regexp_replace("writer", "\s*,\s*", ","), ",")))

df_reviews = df_reviews.join(df_movies.select("movie_id", "writer"), "movie_id")

df_top_critics_reviews = df_reviews.filter(col("isTopCritic") == True)

df_writer_sentiment_counts = df_top_critics_reviews.groupBy("writer", "scoreSentiment") \
    .agg(F.count("*").alias("review_count"))

df_pivoted = df_writer_sentiment_counts.groupBy("writer") \
    .pivot("scoreSentiment", ["POSITIVE", "NEGATIVE"]) \
    .agg(F.sum("review_count").alias("count"))

df_positive_top3 = df_pivoted.orderBy(F.desc("POSITIVE")).limit(3)
df_negative_top3 = df_pivoted.orderBy(F.desc("NEGATIVE")).limit(3)

print("QUERY: Koja 3 filmska pisca su ostavili najvise pozitivnih utisaka po misljenju top kriticara?\n")
df_positive_top3.show()

print("QUERY: Koja 3 filmska pisca su ostavili najvise negativnih utisaka po misljenju top kriticara?\n")
df_negative_top3.show()

df_positive_top3.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX + "_positive", mode="overwrite")
df_negative_top3.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX + "_negative", mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df_positive_top3.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("overwrite") \
    .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
    .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
    .option("mergeSchema", "true") \
    .option('es.index.auto.create', 'true') \
    .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
    .option('es.port', ELASTIC_SEARCH_PORT) \
    .option('es.batch.write.retry.wait', '10s') \
    .save(ELASTIC_SEARCH_INDEX + "_positive")

df_negative_top3.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("overwrite") \
    .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
    .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
    .option("mergeSchema", "true") \
    .option('es.index.auto.create', 'true') \
    .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
    .option('es.port', ELASTIC_SEARCH_PORT) \
    .option('es.batch.write.retry.wait', '10s') \
    .save(ELASTIC_SEARCH_INDEX + "_negative")

current_date = date.today().strftime("%Y/%m/%d")
current_time = datetime.now().strftime("%H:%M:%S")
print(f"{current_date[2:]} {current_time} INFO Added new indices for positive and negative reviews.")
