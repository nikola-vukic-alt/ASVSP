from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, when, col
from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import random
import os

print("Current working directory:", os.getcwd())

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
BASE_PATH = "/asvsp/raw/"

RAW_ZONE_MOVIES_PATH = HDFS_NAMENODE + BASE_PATH + "movies/"
RAW_ZONE_REVIEWS_PATH = HDFS_NAMENODE + BASE_PATH + "reviews/"
RAW_ZONE_MAPPER_PATH = HDFS_NAMENODE + BASE_PATH + "mapper/"
RAW_ZONE_IMDB_MOVIES_PATH = HDFS_NAMENODE + BASE_PATH + "movies_imdb/"
RAW_ZONE_LINKS_PATH = HDFS_NAMENODE + BASE_PATH + "links/"

LOCAL_CSV_MOVIES_FILE="/data/batch/raw_rotten_tomatoes_movies.csv"
LOCAL_CSV_REVIEWS_FILE="/data/batch/raw_rotten_tomatoes_movie_reviews.csv"
LOCAL_CSV_MAPPER_FILE="/data/mapper/raw_movie_mapper.csv"
LOCAL_CSV_IMDB_MOVIES_FILE="/data/mapper/raw_imdb.csv"
LOCAL_CSV_LINKS_FILE="data/streaming/raw_links.csv"

spark = SparkSession.builder \
    .appName("Data Pretransform") \
    .getOrCreate()

df_movies = spark.read.csv(path=LOCAL_CSV_MOVIES_FILE, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=LOCAL_CSV_REVIEWS_FILE, header=True, inferSchema=True)
df_mapper = spark.read.csv(path=LOCAL_CSV_MAPPER_FILE, header=True, inferSchema=True)
df_imdb_movies = spark.read.csv(path=LOCAL_CSV_IMDB_MOVIES_FILE, header=True, inferSchema=True)
df_links = spark.read.csv(path=LOCAL_CSV_LINKS_FILE, header=True, inferSchema=True)

df_movies.write.csv(path=RAW_ZONE_MOVIES_PATH, header=True, mode="overwrite")
df_reviews.write.csv(path=RAW_ZONE_REVIEWS_PATH, header=True, mode="overwrite")
df_mapper.write.csv(path=RAW_ZONE_MAPPER_PATH, header=True, mode="overwrite")
df_imdb_movies.write.csv(path=RAW_ZONE_IMDB_MOVIES_PATH, header=True, mode="overwrite")
df_links.write.csv(path=RAW_ZONE_LINKS_PATH, header=True, mode="overwrite")