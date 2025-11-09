from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

# Build SparkSession
spark = SparkSession.builder.appName("Query4c").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Define Path
MATCHES_PATH = "../data/matches.csv"
MAPS_PATH = "../data/maps.csv"

# Read data as dataframe
matches = spark.read.option("header", "true").option("inferSchema", "true").csv(MATCHES_PATH).withColumnRenamed("mapid","map_id")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv(MAPS_PATH).withColumnRenamed("mapid","map_id")

df_4c = matches.groupBy("map_id").count().orderBy(desc("count")).limit(1)
df_4c = df_4c.join(maps.select("map_id", "name"), "map_id", "left")

df_4c.explain("formatted")
df_4c.show(truncate=False)