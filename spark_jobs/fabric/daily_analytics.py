# Fabric Notebook: Daily Analytics
# Upload this file to a Microsoft Fabric notebook in your workspace.
#
# This notebook reads raw market data from OneLake and computes daily statistics.
# It expects data at: Files/processed/stocks/<SYMBOL>/part-*.parquet
# It writes results to: Files/analytics/daily_stats/
#
# The ABFSS path is constructed automatically from the Fabric Spark session context.
# In Fabric notebooks, 'spark' is pre-initialised; no SparkSession.builder needed.

# ---- Parameters (edit before running) ----
WORKSPACE_ID = spark.conf.get("spark.fabric.workspaceId", "")
LAKEHOUSE_NAME = "MarketDataLakehouse"

# ---- Paths ----
base = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}/Files"
input_path  = f"{base}/processed/stocks/"
output_path = f"{base}/analytics/daily_stats/"

print("=" * 60)
print("Daily Analytics - Microsoft Fabric")
print("=" * 60)
print(f"Input : {input_path}")
print(f"Output: {output_path}")

# ---- Read ----
from pyspark.sql.functions import avg, min, max, stddev, sum, count, col, to_date

df = spark.read.parquet(input_path)
print(f"[OK] Loaded {df.count()} records")

# ---- Compute daily stats ----
daily_stats = df.groupBy(
    "symbol",
    to_date("timestamp").alias("date"),
).agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    stddev("price").alias("volatility"),
    sum("volume").alias("total_volume"),
    count("*").alias("data_points"),
    (max("price") - min("price")).alias("daily_range"),
).withColumn(
    "range_pct",
    col("daily_range") / col("avg_price") * 100,
)

print("\nSample results:")
daily_stats.orderBy(col("date").desc()).show(10, truncate=False)

# ---- Write ----
daily_stats.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)

print(f"\n[OK] Daily analytics written to {output_path}")
