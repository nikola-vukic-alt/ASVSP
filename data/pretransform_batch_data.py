from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year
from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import random
import os
import time

print("Current working directory:", os.getcwd())

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")

MOVIES_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/raw/batch/reviews/"
MAPPER_PATH = HDFS_NAMENODE + "/asvsp/raw/mapper/"

LOCAL_CSV_MOVIES_FILE="/data/batch/raw_rotten_tomatoes_movies.csv"
LOCAL_CSV_REVIEWS_FILE="/data/batch/raw_rotten_tomatoes_movie_reviews.csv"
LOCAL_CSV_MAPPER_FILE="/data/mapper/raw_movie_mapper.csv"

print(f"Trying to access data at:\n{LOCAL_CSV_MOVIES_FILE}\n{LOCAL_CSV_REVIEWS_FILE}")

spark = SparkSession.builder \
    .appName("Data Pretransform") \
    .getOrCreate()

df_reviews = spark.read.csv(path=LOCAL_CSV_REVIEWS_FILE, header=True, inferSchema=True)

def fix_reviews(df_reviews: DataFrame) -> DataFrame:
    
    possible_scores = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    def random_score():
        return random.choice(possible_scores)

    random_score_udf = udf(random_score, FloatType())
    df_reviews = df_reviews.withColumn("originalScore", random_score_udf())

    possible_sentiments = ["POSITIVE", "NEGATIVE"]
    def fix_sentiment(sentiment):
        if sentiment not in possible_sentiments:
            return random.choice(possible_sentiments)
        else:
            return sentiment

    fix_sentiment_udf = udf(fix_sentiment)
    df_reviews = df_reviews.withColumn("scoreSentiment", fix_sentiment_udf("scoreSentiment"))

    return df_reviews

df_reviews = fix_reviews(df_reviews)
df_reviews = df_reviews.drop('criticName', 'reviewText', 'reviewUrl', 'publicatioName', 'reviewState')
df_reviews = df_reviews.withColumn("creationDate", year("creationDate"))
df_reviews = df_reviews.withColumnRenamed("id", "movie_id")

df_mapper = spark.read.csv(path=LOCAL_CSV_MAPPER_FILE, header=True, inferSchema=True)
df_mapper = df_mapper.drop('cast', 'crew')

df_movies = spark.read.csv(path=LOCAL_CSV_MOVIES_FILE, header=True, inferSchema=True)

def movies_fix(df_movies: DataFrame) -> DataFrame:
    def fix_box_office(box_office):
        if box_office is None:
            return 0
        
        box_office = str(box_office)
        if box_office[-1] == 'K':
            return float(box_office[1:-1]) * 1000
        elif box_office[-1] == 'M':
            return float(box_office[1:-1]) * 1000000
        else:
            return 0
    
    fix_box_office_udf = F.udf(fix_box_office, FloatType())
    df_movies = df_movies.withColumn("boxOffice", fix_box_office_udf("boxOffice"))
    
    return df_movies

df_movies = movies_fix(df_movies)
df_movies = df_movies.drop('rating', 'ratingContents', 'distributor', 'soundMix')
df_movies = df_movies.join(df_mapper, df_movies.title == df_mapper.title, "left") \
                             .select(df_movies["*"], df_mapper["movie_id"])
df_movies = df_movies \
    .withColumnRenamed("movie_id", "tmdbId") \
    .withColumnRenamed("id", "movie_id") \

print(f"Trying to overwrite MOVIES on the following path: {MOVIES_PATH}")
df_movies.write.csv(path=MOVIES_PATH, header=True, mode="overwrite")

print(f"Trying to overwrite REVIEWS on the following path: {REVIEWS_PATH}")
df_reviews.write.partitionBy("creationDate").csv(path=REVIEWS_PATH, header=True, mode="overwrite")

print(f"Trying to overwrite MAPPER on the following path: {MAPPER_PATH}")
df_mapper.write.csv(path=MAPPER_PATH, header=True, mode="overwrite")