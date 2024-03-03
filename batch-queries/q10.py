from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, when, expr
from datetime import datetime, date
import time 

spark = SparkSession.builder.appName("Batch Query 10").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/"
ELASTIC_SEARCH_INDEX = "batch_query_10"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

buckets = [(2024, 2020), (2019, 2015), (2014, 2010), (2009, 2005), (2004, 2000)]

df_movies = df_movies.filter(col("releaseDateTheaters").isNotNull())

buckets = [
    (2024, 2020),
    (2019, 2015),
    (2014, 2010),
    (2009, 2005),
    (2004, 2000),
    (1999, 1995),
    (1994, 1990),
    (1989, 1985),
    (1984, 1980),
    (1979, 1975),
    (1974, 1970)
]

def categorize_release_year(release_year):
    for end, start in buckets:
        if start <= release_year <= end:
            return f"{start}-{end}"
    return "Unknown"

categorize_release_year_udf = spark.udf.register("categorize_release_year", categorize_release_year)

df_movies_with_buckets = df_movies.withColumn("release_year_bucket", categorize_release_year_udf(year("releaseDateTheaters")))

df_movies_with_buckets_filtered = df_movies_with_buckets.filter(df_movies_with_buckets.release_year_bucket != "Unknown")

df_combined = df_movies_with_buckets.join(df_reviews, "movie_id", "inner")

df_combined_filtered = df_combined.filter(df_combined.release_year_bucket != "Unknown")

df_bucket_reviews = df_combined_filtered.groupBy("release_year_bucket").agg(
    count(when(col("scoreSentiment") == "POSITIVE", True)).alias("positive_reviews"),
    count("*").alias("total_reviews")
)

df_bucket_percentage = df_bucket_reviews.withColumn(
    "positive_percentage",
    expr("positive_reviews / total_reviews * 100")
).orderBy("positive_percentage", ascending=False)

print("QUERY: Filmovi kog perioda su ostavlili najveći procenat pozitivnih utisaka na kriticare (petogodisnji intervali)?\n")
df_bucket_percentage.show()

df_bucket_percentage.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df_bucket_percentage.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("overwrite") \
    .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
    .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
    .option("mergeSchema", "true") \
    .option('es.index.auto.create', 'true') \
    .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
    .option('es.port', ELASTIC_SEARCH_PORT) \
    .option('es.batch.write.retry.wait', '10s') \
    .save(ELASTIC_SEARCH_INDEX + "_buckets")

current_date = date.today().strftime("%Y/%m/%d")
current_time = datetime.now().strftime("%H:%M:%S")
print(f"{current_date[2:]} {current_time} INFO Added new index: '{ELASTIC_SEARCH_INDEX}'.")

