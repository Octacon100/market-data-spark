-- ============================================================================
-- Athena Tables for Market Data Analytics
-- Run these after Spark jobs complete to query results
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS analytics;

-- ============================================================================
-- Table 1: Daily Statistics
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.daily_stats (
    symbol STRING,
    avg_price DOUBLE,
    min_price DOUBLE,
    max_price DOUBLE,
    volatility DOUBLE,
    total_volume BIGINT,
    data_points BIGINT,
    daily_range DOUBLE,
    range_pct DOUBLE
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://bucket_name/analytics/daily_stats/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Add partitions
MSCK REPAIR TABLE analytics.daily_stats;

-- ============================================================================
-- Table 2: ML Features
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.ml_features (
    timestamp TIMESTAMP,
    price DOUBLE,
    volume BIGINT,
    price_lag_1 DOUBLE,
    price_lag_7 DOUBLE,
    volume_lag_1 BIGINT,
    ma_7d DOUBLE,
    ma_30d DOUBLE,
    volatility_7d DOUBLE,
    volatility_30d DOUBLE,
    volume_ma_7d DOUBLE,
    volume_ma_30d DOUBLE,
    price_momentum_7d DOUBLE,
    price_change DOUBLE
)
PARTITIONED BY (symbol STRING)
STORED AS PARQUET
LOCATION 's3://bucket_name/analytics/ml_features/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Add partitions
MSCK REPAIR TABLE analytics.ml_features;

-- ============================================================================
-- Table 3: Volatility Metrics
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.volatility_metrics (
    symbol STRING,
    daily_volatility DOUBLE,
    intraday_range_pct DOUBLE,
    atr_approx DOUBLE,
    volume_volatility DOUBLE,
    large_moves_count BIGINT,
    annualized_volatility DOUBLE,
    volatility_rank INT
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://bucket_name/analytics/volatility_metrics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Add partitions
MSCK REPAIR TABLE analytics.volatility_metrics;

-- ============================================================================
-- Example Queries
-- ============================================================================

-- Query 1: Latest daily statistics
SELECT 
    symbol,
    date,
    avg_price,
    volatility,
    total_volume
FROM analytics.daily_stats
WHERE date >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
ORDER BY date DESC, symbol;

-- Query 2: Most volatile stocks
SELECT 
    symbol,
    date,
    annualized_volatility,
    intraday_range_pct,
    volatility_rank
FROM analytics.volatility_metrics
WHERE date = (SELECT MAX(date) FROM analytics.volatility_metrics)
ORDER BY annualized_volatility DESC;

-- Query 3: ML features for specific symbol
SELECT 
    symbol,
    timestamp,
    price,
    ma_7d,
    ma_30d,
    price_momentum_7d,
    volatility_7d
FROM analytics.ml_features
WHERE symbol = 'AAPL'
ORDER BY timestamp DESC
LIMIT 100;

-- Query 4: Cross-symbol comparison
SELECT 
    d.symbol,
    d.date,
    d.avg_price,
    d.volatility,
    v.annualized_volatility,
    v.volatility_rank
FROM analytics.daily_stats d
JOIN analytics.volatility_metrics v
    ON d.symbol = v.symbol AND d.date = v.date
WHERE d.date = (SELECT MAX(date) FROM analytics.daily_stats)
ORDER BY v.annualized_volatility DESC;

-- Query 5: Price momentum trends
SELECT 
    symbol,
    DATE(timestamp) as date,
    AVG(price_momentum_7d) as avg_momentum,
    MAX(price_momentum_7d) as max_momentum,
    MIN(price_momentum_7d) as min_momentum
FROM analytics.ml_features
WHERE symbol IN ('AAPL', 'GOOGL', 'MSFT')
GROUP BY symbol, DATE(timestamp)
ORDER BY date DESC, symbol;
