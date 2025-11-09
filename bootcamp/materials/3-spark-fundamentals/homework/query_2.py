from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Build SparkSession
spark = SparkSession.builder.appName('Query2').getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Define data path
MEDALS_PATH = "../data/medals.csv"
MAPS_PATH = "../data/maps.csv"
MATCHES_PATH = "../data/matches.csv"
MEDAL_MATCHES_PLAYERS_PATH = "../data/medals_matches_players.csv"

# Read file as Dataframe
medals = spark.read.option("header", "true").csv(MEDALS_PATH)
maps = spark.read.option("header", "true").csv(MAPS_PATH)
matches = spark.read.option("header", "true").csv(MATCHES_PATH)
medal_matches_players = spark.read.option("header", "true").csv(MEDAL_MATCHES_PLAYERS_PATH)

# Explicit Broadcast Join
# Join medal_matches_players (large) with medals (small)
medal_broadcast_join_df = medal_matches_players.join(
    broadcast(medals),
    "medal_id",
    "left"
)

# Join matches (large) with maps (small)
match_broadcast_join_df = matches.join(
    broadcast(maps),
    "mapid",
    "left"
)

broadcast_join_df = match_broadcast_join_df.join(
    medal_broadcast_join_df,
    "match_id",
    "inner"
)

# Verify the plan shows BroadcastHashJoin
broadcast_join_df.explain("formatted")