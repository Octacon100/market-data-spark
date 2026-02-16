"""
Spark Job: ML Features
Generate machine learning features from market data
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main(bucket_name):
    """
    Generate ML features with window functions
    
    Args:
        bucket_name: S3 bucket name
    """
    
    print("="*60)
    print("Starting ML Features Spark Job")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("MarketData-MLFeatures") \
        .getOrCreate()
    
    try:
        # Read data
        input_path = f"s3://{bucket_name}/processed/stocks/"
        print(f"Reading data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        # Define windows
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        window_7d = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-6, 0)
        window_30d = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)
        
        # Generate features
        print("Computing features...")
        
        ml_features = df \
            .withColumn("price_lag_1", lag("price", 1).over(window_spec)) \
            .withColumn("price_lag_7", lag("price", 7).over(window_spec)) \
            .withColumn("volume_lag_1", lag("volume", 1).over(window_spec)) \
            .withColumn("ma_7d", avg("price").over(window_7d)) \
            .withColumn("ma_30d", avg("price").over(window_30d)) \
            .withColumn("volatility_7d", stddev("price").over(window_7d)) \
            .withColumn("volatility_30d", stddev("price").over(window_30d)) \
            .withColumn("volume_ma_7d", avg("volume").over(window_7d)) \
            .withColumn("volume_ma_30d", avg("volume").over(window_30d))
        
        # Calculate momentum
        ml_features = ml_features.withColumn(
            "price_momentum_7d",
            when(col("price_lag_7").isNotNull(),
                 (col("price") - col("price_lag_7")) / col("price_lag_7") * 100)
            .otherwise(0)
        )
        
        # Calculate RSI-like indicator (simplified)
        ml_features = ml_features.withColumn(
            "price_change",
            col("price") - col("price_lag_1")
        )
        
        # Drop nulls (from lagged values)
        ml_features = ml_features.dropna()
        
        # Write results
        output_path = f"s3://{bucket_name}/analytics/ml_features/"
        print(f"Writing results to: {output_path}")
        
        ml_features.write \
            .mode("overwrite") \
            .partitionBy("symbol") \
            .parquet(output_path)
        
        # Show sample
        print("\nSample features:")
        ml_features.select(
            "symbol", "timestamp", "price",
            "ma_7d", "ma_30d", "volatility_7d", "price_momentum_7d"
        ).orderBy(col("timestamp").desc()).show(10, truncate=False)
        
        print(f"\n✅ ML features completed! Records: {ml_features.count()}")
        
    except Exception as e:
        print(f"\n❌ Error in ML features: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML feature engineering")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--S3_BUCKET", help="S3 bucket name (Glue argument)")
    args, _ = parser.parse_known_args()

    bucket = args.bucket or args.S3_BUCKET
    if not bucket:
        parser.error("--bucket or --S3_BUCKET is required")
    main(bucket)
