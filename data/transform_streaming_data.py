from pyspark.sql import SparkSession
import time
from os import environ

spark = SparkSession.builder.appName("Streaming Reviews").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
BASE_PATH = "/asvsp/raw/"

RAW_ZONE_LINKS_PATH = HDFS_NAMENODE + BASE_PATH + "links/"
RAW_ZONE_STREAMING_MOVIES_PATH = HDFS_NAMENODE + BASE_PATH + "streaming/movies/"
RAW_ZONE_STREAMING_REVIEWS_PATH = HDFS_NAMENODE + BASE_PATH + "streaming/ratings/"

# Read the dataframes
df_reviews = spark.read.csv(path=RAW_ZONE_STREAMING_REVIEWS_PATH, header=True, inferSchema=True)
df_movies = spark.read.csv(path=RAW_ZONE_STREAMING_MOVIES_PATH, header=True, inferSchema=True)
df_links = spark.read.csv(path=RAW_ZONE_LINKS_PATH, header=True, inferSchema=True)

# Join df_reviews with df_links and df_movies based on the movieId column
df_joined = df_reviews.join(df_links.select("movieId", "tmdbId", "imdbId"), "movieId").join(df_movies.select("movieId", "title", "genres"), "movieId")

# Select the desired columns from the final joined dataframe
df_reviews_final = df_joined.select("userId", "movieId", "rating", "title", "genres", "tmdbId", "imdbId", "timestamp")

# # Show the final dataframe
# df_reviews_final.show()

# count_unique_imdbId = df_reviews_final.select("imdbId").distinct().count()
# print(f"Total unique imdbId: {count_unique_imdbId}")

# # Sleep for 10 seconds (optional)
# time.sleep(10)

# Write the final dataframe to a CSV file
df_reviews_final.write.csv("/data/streaming/reviews.csv", header=True, mode="overwrite")
