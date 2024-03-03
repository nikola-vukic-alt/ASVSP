from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime, date

spark = SparkSession.builder.appName("Batch Query 4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_4"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

df_movies = df_movies.withColumn("writer", explode(split(regexp_replace("writer", "\s*,\s*", ","), ",")))

df_certified = df_reviews.groupBy("movie_id").agg(
    F.count(F.when(col("scoreSentiment") == "POSITIVE", True)).alias("positive_reviews"),
    F.sum(F.when(col("isTopCritic") == True, 1).otherwise(0)).alias("top_critic_reviews")
)

df_certified = df_certified.filter(
    (col("positive_reviews") / col("top_critic_reviews")) >= 0.75) \
    .filter(col("top_critic_reviews") >= 5) \
    .filter(col("positive_reviews") >= 80)

df_certified = df_certified.join(df_movies, "movie_id")

df_top_writers = df_certified.groupBy("writer").agg(F.count("*").alias("num_movies"))

df_top5_writers = df_top_writers.orderBy(F.desc("num_movies")).limit(5)

# Extracting the top 5 writer names
top_writers_list = [row['writer'] for row in df_top5_writers.collect()]

# Filtering the certified dataframe for the top 5 writers
df_certified_top5 = df_certified.filter(col("writer").isin(top_writers_list))

# Splitting genres and counting occurrences
df_genres = df_certified_top5.withColumn("genre", explode(split(col("genre"), ", "))) \
    .groupBy("writer", "genre") \
    .agg(F.count("*").alias("genre_count"))

# Ranking genres based on count within each writer
window_spec = Window.partitionBy("writer").orderBy(F.desc("genre_count"))

df_genres_ranked = df_genres.withColumn("genre_rank", F.row_number().over(window_spec))

# Filtering for the top 3 genres for each writer
df_top3_genres = df_genres_ranked.filter(col("genre_rank") <= 3)

print("QUERY: Kojih je tri najpopularnija zanra za pisanje 5 pisaca koji su napisali najvise certified fresh filmova?\n")
df_top3_genres.show()

df_top3_genres.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df_top3_genres.write \
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

# Print information
current_date = date.today().strftime("%Y/%m/%d")
current_time = datetime.now().strftime("%H:%M:%S")
print(f"{current_date[2:]} {current_time} INFO Added new index: '{ELASTIC_SEARCH_INDEX}'.")
