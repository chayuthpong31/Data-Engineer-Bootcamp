from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc, broadcast

# Build SparkSession
spark = SparkSession.builder.appName("Query4d").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Define Path
MEDALS_PATH = "../data/medals.csv"
MEDALS_MATCHES_PLAYERS_PATH = "../data/medals_matches_players.csv"
MATCHES_PATH = "../data/matches.csv"

# Read data as dataframe
medals = spark.read.option("header", "true").option("inferSchema", "true").csv(MEDALS_PATH)
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv(MEDALS_MATCHES_PLAYERS_PATH)
matches = spark.read.option("header", "true").option("inferSchema", "true").csv(MATCHES_PATH).withColumnRenamed("mapid", "map_id")

medals_b = broadcast(medals.select("medal_id", "name"))

df_4d = medals_matches_players \
        .join(medals_b, "medal_id", "inner") \
        .filter(col("name") == "Killing Spree") \
        .join(matches.select("match_id", "map_id"), "match_id", "inner") \
        .groupBy("map_id").count().orderBy(desc("count")).limit(1)

df_4d.explain("formatted")
df_4d.show(truncate=False)