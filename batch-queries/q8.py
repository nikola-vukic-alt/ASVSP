from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 8").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_8"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

df_certified = df_reviews \
    .dropna(how='any', subset=['isTopCritic']) \
    .groupBy("movie_id").agg(
        F.count(F.when(col("scoreSentiment") == "POSITIVE", True)).alias("positive_reviews"),
        F.count(F.when(col("isTopCritic") == True, True)).alias("top_critic_reviews"),
        F.count("*").alias("total_reviews")
    )


df_certified = df_certified.filter(
    (col("positive_reviews") / col("total_reviews")) >= 0.75) \
    .filter(col("top_critic_reviews") >= 5) \
    .filter(col("positive_reviews") >= 80)

df_certified = df_certified.join(df_movies, "movie_id")

df_top_movies = df_certified.orderBy(F.desc("total_reviews")).limit(10)

print("QUERY: Kojih 10 certified fresh filmova ima najvise kritika?\n")
df_top_movies.dropDuplicates(["title"]).select("title", "total_reviews").show()

df_top_movies.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df_top_movies.write \
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
