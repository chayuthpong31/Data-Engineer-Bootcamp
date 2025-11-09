from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder.appName("Query5c_Optimized")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "64") 
    .getOrCreate()
)

MATCHES_PATH = "../data/matches.csv"
PATH_C = "../data/data_optimization_results/version_c"

matches = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(MATCHES_PATH)
    .withColumnRenamed("mapid", "map_id")
)

print(f"Loaded matches: {matches.count():,} rows")

(
    matches
    .repartition(64, "playlist_id", "map_id") 
    .sortWithinPartitions("match_id")
    .write.mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("playlist_id", "map_id")
    .parquet(PATH_C)
)

print(f"Wrote Parquet files to {PATH_C}")

total_bytes = 0
file_count = 0
for root, _, files in os.walk(PATH_C):
    for f in files:
        fp = os.path.join(root, f)
        if fp.endswith(".parquet"):
            total_bytes += os.path.getsize(fp)
            file_count += 1

avg_file_size = total_bytes / file_count if file_count > 0 else 0

print(f"Total bytes written: {total_bytes:,} bytes ({total_bytes/1024/1024:.2f} MB)")
print(f"Total parquet files: {file_count}")
print(f"Average file size:   {avg_file_size/1024/1024:.2f} MB")