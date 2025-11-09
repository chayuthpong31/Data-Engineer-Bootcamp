from pyspark.sql import SparkSession

# Build spark session
spark = SparkSession.builder.appName('Query1').getOrCreate()

# Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "16")

print(f"Auto Broadcast Threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")