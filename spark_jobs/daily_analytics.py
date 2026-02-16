"""
Spark Job: Daily Analytics
Compute daily statistics for market data
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

def main(bucket_name):
    """
    Compute daily analytics on market data
    
    Args:
        bucket_name: S3 bucket name
    """
    
    print("="*60)
    print("Starting Daily Analytics Spark Job")
    print("="*60)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("MarketData-DailyAnalytics") \
        .getOrCreate()
    
    try:
        # Read processed data
        input_path = f"s3://{bucket_name}/processed/stocks/"
        print(f"Reading data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        print(f"Records loaded: {df.count()}")
        
        # Compute daily statistics
        daily_stats = df.groupBy(
            "symbol",
            to_date("timestamp").alias("date")
        ).agg(
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            stddev("price").alias("volatility"),
            sum("volume").alias("total_volume"),
            count("*").alias("data_points"),
            (max("price") - min("price")).alias("daily_range")
        )
        
        # Calculate additional metrics
        daily_stats = daily_stats.withColumn(
            "range_pct",
            (col("daily_range") / col("avg_price") * 100)
        )
        
        # Write results
        output_path = f"s3://{bucket_name}/analytics/daily_stats/"
        print(f"Writing results to: {output_path}")
        
        daily_stats.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(output_path)
        
        # Show sample
        print("\nSample results:")
        daily_stats.orderBy(col("date").desc()).show(10, truncate=False)
        
        print("\n✅ Daily analytics completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error in daily analytics: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily market data analytics")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--S3_BUCKET", help="S3 bucket name (Glue argument)")
    args, _ = parser.parse_known_args()

    bucket = args.bucket or args.S3_BUCKET
    if not bucket:
        parser.error("--bucket or --S3_BUCKET is required")
    main(bucket)
