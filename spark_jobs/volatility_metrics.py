"""
Spark Job: Volatility Metrics
Calculate advanced volatility metrics
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from math import sqrt

def main(bucket_name):
    """
    Calculate volatility metrics
    
    Args:
        bucket_name: S3 bucket name
    """
    
    print("="*60)
    print("Starting Volatility Metrics Spark Job")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("MarketData-VolatilityMetrics") \
        .getOrCreate()
    
    try:
        # Read data
        input_path = f"s3://{bucket_name}/processed/stocks/"
        print(f"Reading data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        # Windows for different timeframes
        window_7d = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-6, 0)
        window_30d = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)
        
        # Calculate returns
        df = df.withColumn(
            "log_return",
            log(col("price") / lag("price", 1).over(Window.partitionBy("symbol").orderBy("timestamp")))
        )
        
        # Volatility metrics
        print("Computing volatility metrics...")
        
        volatility = df.groupBy("symbol", to_date("timestamp").alias("date")).agg(
            # Basic volatility
            stddev("log_return").alias("daily_volatility"),
            
            # Intraday volatility
            ((max("high") - min("low")) / avg("price") * 100).alias("intraday_range_pct"),
            
            # Average true range approximation
            avg(
                greatest(
                    col("high") - col("low"),
                    abs(col("high") - col("previous_close")),
                    abs(col("low") - col("previous_close"))
                )
            ).alias("atr_approx"),
            
            # Volume volatility
            stddev("volume").alias("volume_volatility"),
            
            # Number of price changes > 1%
            sum(when(abs(col("price_change_pct")) > 1, 1).otherwise(0)).alias("large_moves_count")
        )
        
        # Annualize volatility (252 trading days)
        volatility = volatility.withColumn(
            "annualized_volatility",
            col("daily_volatility") * sqrt(252)
        )
        
        # Volatility ranking
        window_rank = Window.partitionBy("date").orderBy(col("annualized_volatility").desc())
        volatility = volatility.withColumn(
            "volatility_rank",
            row_number().over(window_rank)
        )
        
        # Write results
        output_path = f"s3://{bucket_name}/analytics/volatility_metrics/"
        print(f"Writing results to: {output_path}")
        
        volatility.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(output_path)
        
        # Show sample
        print("\nSample volatility metrics:")
        volatility.orderBy(col("date").desc(), col("volatility_rank")) \
            .show(10, truncate=False)
        
        print("\n✅ Volatility metrics completed!")
        
    except Exception as e:
        print(f"\n❌ Error in volatility metrics: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Volatility metrics calculation")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--S3_BUCKET", help="S3 bucket name (Glue argument)")
    args, _ = parser.parse_known_args()

    bucket = args.bucket or args.S3_BUCKET
    if not bucket:
        parser.error("--bucket or --S3_BUCKET is required")
    main(bucket)
