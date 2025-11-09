from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc

# Build SparkSession
spark = SparkSession.builder.appName("Query4a").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Define Path
MATCH_DETAILS_PATH = "../data/match_details.csv"

# Read data as dataframe
match_details = spark.read.option("header", "true").option("inferSchema","true").csv(MATCH_DETAILS_PATH)

df_4a = match_details.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills")).orderBy(desc("avg_kills")).limit(1)

df_4a.explain("formatted")
df_4a.show(truncate=False)