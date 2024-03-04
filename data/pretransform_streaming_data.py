from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Streaming Reviews").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_reviews = spark.read.csv(path="/data/streaming/raw_ratings.csv", header=True, inferSchema=True)
df_movies = spark.read.csv(path="/data/streaming/raw_movies.csv", header=True, inferSchema=True)
df_links = spark.read.csv(path="/data/streaming/raw_links.csv", header=True, inferSchema=True)

df_joined = df_reviews.join(df_movies, "movieId").join(df_links.select("movieId", "tmdbId"), "movieId")

df_reviews_final = df_joined.select("userId", "movieId", "rating", "title", "genres", "tmdbId", "timestamp")

# print("Reviews:")
# df_reviews_final.show()

df_reviews_final.write.csv("/data/streaming/reviews.csv", header=True, mode="overwrite")
