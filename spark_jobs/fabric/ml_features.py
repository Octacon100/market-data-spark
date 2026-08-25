# Fabric Notebook: ML Features
# Upload this file to a Microsoft Fabric notebook in your workspace.
#
# Reads processed stock parquet from OneLake, computes ML features using
# window functions (moving averages, momentum, volatility), and writes
# the feature table back to OneLake partitioned by symbol.
#
# Input:  Files/processed/stocks/<SYMBOL>/part-*.parquet
# Output: Files/analytics/ml_features/symbol=<SYMBOL>/part-*.parquet

# ---- Parameters ----
WORKSPACE_ID = spark.conf.get("spark.fabric.workspaceId", "")
LAKEHOUSE_NAME = "MarketDataLakehouse"

# ---- Paths ----
base = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}/Files"
input_path  = f"{base}/processed/stocks/"
output_path = f"{base}/analytics/ml_features/"

print("=" * 60)
print("ML Features - Microsoft Fabric")
print("=" * 60)
print(f"Input : {input_path}")
print(f"Output: {output_path}")

from pyspark.sql.functions import avg, stddev, lag, col, when
from pyspark.sql.window import Window

df = spark.read.parquet(input_path)
print(f"[OK] Loaded {df.count()} records")

# Window specs
window_spec = Window.partitionBy("symbol").orderBy("timestamp")
window_7d   = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-6, 0)
window_30d  = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)

ml_features = (
    df
    .withColumn("price_lag_1",   lag("price", 1).over(window_spec))
    .withColumn("price_lag_7",   lag("price", 7).over(window_spec))
    .withColumn("volume_lag_1",  lag("volume", 1).over(window_spec))
    .withColumn("ma_7d",         avg("price").over(window_7d))
    .withColumn("ma_30d",        avg("price").over(window_30d))
    .withColumn("volatility_7d", stddev("price").over(window_7d))
    .withColumn("volatility_30d",stddev("price").over(window_30d))
    .withColumn("volume_ma_7d",  avg("volume").over(window_7d))
    .withColumn("volume_ma_30d", avg("volume").over(window_30d))
    .withColumn(
        "price_momentum_7d",
        when(col("price_lag_7").isNotNull(),
             (col("price") - col("price_lag_7")) / col("price_lag_7") * 100
        ).otherwise(0),
    )
    .withColumn("price_change", col("price") - col("price_lag_1"))
    .dropna()
)

print("\nSample features:")
ml_features.select(
    "symbol", "timestamp", "price", "ma_7d", "ma_30d", "volatility_7d", "price_momentum_7d"
).orderBy(col("timestamp").desc()).show(10, truncate=False)

ml_features.write \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .parquet(output_path)

print(f"\n[OK] ML features written to {output_path} ({ml_features.count()} records)")
