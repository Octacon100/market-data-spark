# Fabric Notebook: Volatility Metrics
# Upload this file to a Microsoft Fabric notebook in your workspace.
#
# Reads processed stock parquet from OneLake and computes advanced volatility
# metrics (daily volatility, intraday range, ATR approximation, Bollinger Bands).
#
# Input:  Files/processed/stocks/<SYMBOL>/part-*.parquet
# Output: Files/analytics/volatility_metrics/

# ---- Parameters ----
WORKSPACE_ID = spark.conf.get("spark.fabric.workspaceId", "")
LAKEHOUSE_NAME = "MarketDataLakehouse"

# ---- Paths ----
base = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}/Files"
input_path  = f"{base}/processed/stocks/"
output_path = f"{base}/analytics/volatility_metrics/"

print("=" * 60)
print("Volatility Metrics - Microsoft Fabric")
print("=" * 60)
print(f"Input : {input_path}")
print(f"Output: {output_path}")

from pyspark.sql.functions import (
    avg, stddev, max, min, abs, col, lag, log, greatest,
    to_date, lit, sqrt
)
from pyspark.sql.window import Window

df = spark.read.parquet(input_path)
print(f"[OK] Loaded {df.count()} records")

window_spec = Window.partitionBy("symbol").orderBy("timestamp")
window_7d   = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-6, 0)
window_30d  = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)

df = df.withColumn(
    "log_return",
    log(col("price") / lag("price", 1).over(window_spec)),
)

volatility = df.groupBy("symbol", to_date("timestamp").alias("date")).agg(
    stddev("log_return").alias("daily_volatility"),
    ((max("high") - min("low")) / avg("price") * 100).alias("intraday_range_pct"),
    avg(
        greatest(
            col("high") - col("low"),
            abs(col("high") - col("previous_close")),
            abs(col("low") - col("previous_close")),
        )
    ).alias("avg_true_range"),
    avg("volume").alias("avg_volume"),
    max("price").alias("period_high"),
    min("price").alias("period_low"),
)

# Annualised volatility (252 trading days)
volatility = volatility.withColumn(
    "annualized_volatility",
    col("daily_volatility") * sqrt(lit(252)),
)

print("\nSample results:")
volatility.orderBy(col("date").desc()).show(10, truncate=False)

volatility.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)

print(f"\n[OK] Volatility metrics written to {output_path}")
