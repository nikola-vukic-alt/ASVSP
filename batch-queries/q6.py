from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 6").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_6"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

df_positive_reviews = df_reviews.filter(col("scoreSentiment") == "POSITIVE")

df_review_counts = df_positive_reviews.groupBy("movie_id").agg(F.count("*").alias("total_reviews"))

df_positive_review_counts = df_positive_reviews.groupBy("movie_id").agg(F.count("*").alias("positive_reviews"))

df_review_counts = df_review_counts.join(df_positive_review_counts, "movie_id", "left")

df_review_counts = df_review_counts.withColumn("positive_review_percentage", 
                                               F.col("positive_reviews") / F.col("total_reviews") * 100)

df_review_counts = df_review_counts.filter(F.col("total_reviews") >= 100)

window_spec = Window.orderBy(F.desc("total_reviews"))
df_ranked_movies = df_review_counts.withColumn("rank", F.rank().over(window_spec))

df_directors = df_ranked_movies.join(df_movies.select("movie_id", "director", "title"), "movie_id", "inner")

df_top_directors = df_directors.filter(F.col("rank") <= 5).select("director", "positive_review_percentage", "title", "total_reviews")

df_top_directors = df_top_directors.dropDuplicates()

print("QUERY: Ko su reditelji filmova sa top 5 najvecih procenata pozitivnih utisaka a da imaju barem 100 kritika?\n")
df_top_directors.show()

df_top_directors.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df_top_directors.write \
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
