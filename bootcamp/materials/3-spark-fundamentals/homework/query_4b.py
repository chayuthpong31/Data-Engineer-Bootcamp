from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, col

# Build SparkSession
spark = SparkSession.builder.appName("Query4b").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Define Path
MATCHES_PATH = "../data/matches.csv"
MAPS_PATH = "../data/maps.csv"

# Read data as dataframe
matches = spark.read.option("header", "true").option("inferSchema", "true").csv(MATCHES_PATH).withColumnRenamed("mapid","map_id")

df_4b = matches.groupBy("playlist_id").count().orderBy(desc("count")).limit(1)

df_4b.explain("formatted")
df_4b.show(truncate=False)