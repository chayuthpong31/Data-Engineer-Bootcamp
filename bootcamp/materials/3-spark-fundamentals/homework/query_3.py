from pyspark.sql import SparkSession

# Build SparkSession
spark = SparkSession.builder.appName("Query3").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.sql.bucketing.enabled", "true")

# Define path
MATCH_DETAILS_PATH = "../data/match_details.csv"
MATCHES_PATH = "../data/matches.csv"
MEDAL_MATCHES_PLAYERS_PATH = "../data/medals_matches_players.csv"

# Read data from file as dataframe
match_details = spark.read.option("header", "true").csv(MATCH_DETAILS_PATH)
matches = spark.read.option("header", "true").csv(MATCHES_PATH)
medal_matches_players = spark.read.option("header", "true").csv(MEDAL_MATCHES_PLAYERS_PATH)

# Write bucketed table
BUCKETS = 16
BUCKET_COLUMN = "match_id"

def write_bucketed(df, name):
    df.write.mode("overwrite")\
    .bucketBy(BUCKETS, BUCKET_COLUMN) \
    .sortBy(BUCKET_COLUMN) \
    .saveAsTable(name)

write_bucketed(match_details, "bd_match_details")
write_bucketed(matches, "bd_matches")
write_bucketed(medal_matches_players, "bd_medal_matches_players")

# Read back & join verification
df_match_details_bucketed = spark.table("bd_match_details")
df_matches_bucketed = spark.table("bd_matches")
df_medal_matches_players_bucketed = spark.table("bd_medal_matches_players")

# Join the bucketed table
bucket_join_df = df_match_details_bucketed.join(
    df_matches_bucketed,
    BUCKET_COLUMN,
    "inner"
).join(
    df_medal_matches_players_bucketed,
    BUCKET_COLUMN,
    "inner"
)

# Verify
bucket_join_df.explain("formatted")