from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime, date

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIES_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/movies/"
REVIEWS_PATH = HDFS_NAMENODE + "/asvsp/transform/batch/reviews/"
OUTPUT_PATH = HDFS_NAMENODE + "/asvsp/curated/batch/"
    
spark = SparkSession \
    .builder \
    .appName("Batch Query 1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
ELASTIC_SEARCH_INDEX = "batch_query_1"

df_movies = spark.read.csv(path=MOVIES_PATH, header=True, inferSchema=True)
df_reviews = spark.read.csv(path=REVIEWS_PATH, header=True, inferSchema=True)

# Koji su najkritikovaniji zanrovi filmova za posljednjih 15 godina?
df = df_movies.join(df_reviews, "movie_id") \
    .withColumn("genre", explode(split(regexp_replace("genre", "\s*,\s*", ","), ","))) \
    .groupBy("creationDate", "genre") \
    .count()

windowSpec = Window.partitionBy("creationDate").orderBy(F.desc("count"))
df = df.withColumn("rank", F.rank().over(windowSpec)) \
    .filter(col("rank") == 1) \
    .select("creationDate", "genre", "count") \
    .orderBy(F.desc("count")) \
    .limit(15)

print("QUERY: Koji su najkritikovaniji zanrovi filmova za posljednjih 15 godina?\n")
df.show()

df.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

df.write \
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