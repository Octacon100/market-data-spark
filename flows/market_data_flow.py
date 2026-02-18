"""
Market Data Pipeline with Prefect 3 - Production Ready
Supports: Stocks, Crypto (coming), Futures (coming)
Full asset tracking and lineage in Prefect Cloud
"""

from prefect import flow, task
from prefect.assets import materialize
from prefect.artifacts import create_link_artifact, create_markdown_artifact
from prefect.events import emit_event
from prefect.transactions import transaction
import boto3
import requests
from datetime import datetime
import json
import pandas as pd
import io
import os
from typing import List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import dotenv

dotenv.load_dotenv()

# ============================================================================
# Watchlist Loader
# ============================================================================

WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "watchlist.json"


def load_watchlist() -> list:
    """Load stock symbols from config/watchlist.json (plain list of tickers)"""
    if WATCHLIST_PATH.exists():
        with open(WATCHLIST_PATH, "r") as f:
            return json.load(f)
    return []


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class PipelineConfig:
    """Centralized configuration"""
    alpha_vantage_key: str = os.getenv('ALPHA_VANTAGE_API_KEY', '')
    s3_bucket: str = os.getenv('S3_BUCKET', '')
    symbols: List[str] = None
    pipeline_version: str = "1.0.0"

    def __post_init__(self):
        if self.symbols is None:
            # Priority: 1) SYMBOLS env var  2) watchlist.json  3) hardcoded default
            env_symbols = os.getenv('SYMBOLS', '')
            if env_symbols:
                self.symbols = [s.strip() for s in env_symbols.split(',') if s.strip()]
            else:
                watchlist = load_watchlist()
                if watchlist:
                    self.symbols = watchlist
                else:
                    self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

        # Validation
        if not self.alpha_vantage_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set")
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable not set")

config = PipelineConfig()
s3 = boto3.client('s3')


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class StockData:
    """Stock market data model"""
    symbol: str
    timestamp: str
    price: float
    volume: int
    open: float
    high: float
    low: float
    previous_close: float
    change: float
    change_percent: str
    source: str = "alphavantage"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AssetMetadata:
    """Metadata for tracked assets"""
    symbol: str
    uri: str
    s3_key: str
    asset_id: str
    timestamp: str
    console_url: str
    format: str
    size_bytes: int
    upstream_asset_id: Optional[str] = None


# ============================================================================
# Data Fetching Tasks
# ============================================================================

@task(
    retries=3,
    retry_delay_seconds=60,
    log_prints=True,
    tags=["api", "fetch", "alphavantage"]
)
def fetch_stock_price(symbol: str) -> StockData:
    """
    Fetch real-time stock price from Alpha Vantage API

    Features:
    - Automatic retries on failure
    - Rate limit detection
    - Error handling
    - Structured response

    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL')

    Returns:
        StockData: Structured stock data

    Raises:
        ValueError: If API returns error or invalid data
    """

    print(f"ðŸ“ˆ Fetching {symbol} from Alpha Vantage...")

    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': config.alpha_vantage_key
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Error handling
        if 'Error Message' in data:
            raise ValueError(f"API Error for {symbol}: {data['Error Message']}")

        if 'Note' in data:
            # Rate limit hit - this will trigger retry
            print(f"  âš ï¸  Rate limit detected for {symbol}, will retry...")
            raise ValueError(f"Rate limit hit for {symbol}")

        quote = data.get('Global Quote', {})
        if not quote:
            raise ValueError(f"No quote data returned for {symbol}")

        # Parse response into structured data
        stock_data = StockData(
            symbol=symbol,
            timestamp=datetime.now().isoformat(),
            price=float(quote.get('05. price', 0)),
            volume=int(quote.get('06. volume', 0)),
            open=float(quote.get('02. open', 0)),
            high=float(quote.get('03. high', 0)),
            low=float(quote.get('04. low', 0)),
            previous_close=float(quote.get('08. previous close', 0)),
            change=float(quote.get('09. change', 0)),
            change_percent=quote.get('10. change percent', '0%')
        )

        print(f"  âœ“ {symbol}: ${stock_data.price:.2f} ({stock_data.change_percent})")

        return stock_data

    except requests.exceptions.RequestException as e:
        print(f"  âŒ Network error fetching {symbol}: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"  âŒ Data parsing error for {symbol}: {e}")
        raise


# ============================================================================
# Data Quality Tasks
# ============================================================================

@task(log_prints=True, tags=["validation", "quality"])
def validate_data(data: StockData) -> StockData:
    """
    Validate data quality with multiple checks

    Quality Checks:
    1. Price must be positive
    2. Volume must be non-negative
    3. Detect large price movements (>20%)
    4. Emit events for anomalies

    Args:
        data: Stock data to validate

    Returns:
        StockData: Validated data (unchanged)

    Raises:
        ValueError: If validation fails
    """

    symbol = data.symbol

    # Check 1: Price validation
    if data.price <= 0:
        error_msg = f"Invalid price for {symbol}: ${data.price}"
        print(f"  âŒ {error_msg}")
        raise ValueError(error_msg)

    # Check 2: Volume validation
    if data.volume < 0:
        error_msg = f"Invalid volume for {symbol}: {data.volume}"
        print(f"  âŒ {error_msg}")
        raise ValueError(error_msg)

    # Check 3: Detect anomalies (large price movements)
    if data.previous_close and data.price:
        change_pct = abs((data.price - data.previous_close) / data.previous_close)

        if change_pct > 0.20:  # >20% movement
            severity = "high" if change_pct > 0.30 else "medium"
            print(f"  âš ï¸  Large price movement detected: {change_pct*100:.1f}% (severity: {severity})")

            # Emit anomaly event to Prefect Cloud
            emit_event(
                event="market.price.anomaly",
                resource={
                    "prefect.resource.id": f"stock.{symbol}",
                    "prefect.resource.name": symbol
                },
                payload={
                    "symbol": symbol,
                    "change_percent": round(change_pct * 100, 2),
                    "old_price": data.previous_close,
                    "new_price": data.price,
                    "severity": severity,
                    "threshold": 20.0
                }
            )

    print(f"  âœ“ Validation passed for {symbol}")

    return data


# ============================================================================
# Storage Tasks
# ============================================================================

@materialize(
    "s3://s3bucket/stocks/xxx",
    log_prints=True,
    tags=["storage", "s3", "raw", "asset"]
)
def store_to_s3_raw(data: StockData) -> AssetMetadata:
    """
    Store raw JSON to S3 with partitioning and asset registration

    Storage Structure:
    s3://bucket/stocks/date=YYYY-MM-DD/hour=HH/SYMBOL_YYYYMMDD_HHMMSS.json

    Features:
    - Partitioned by date and hour (optimized for Athena)
    - Creates clickable link artifact in Prefect UI
    - Emits asset creation event
    - Includes metadata tags

    Args:
        data: Stock data to store

    Returns:
        AssetMetadata: Asset metadata for downstream tasks
    """

    symbol = data.symbol
    now = datetime.now()

    # Create partitioned S3 key
    s3_key = (
        f"stocks/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"hour={now.strftime('%H')}/"
        f"{symbol}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    # Convert to JSON
    json_data = json.dumps(data.to_dict(), indent=2)

    try:
        # Upload to S3 with metadata
        s3.put_object(
            Bucket=config.s3_bucket,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json',
            Metadata={
                'symbol': symbol,
                'timestamp': now.isoformat(),
                'pipeline': 'market-data',
                'format': 'json',
                'version': config.pipeline_version
            }
        )

        s3_uri = f"s3://{config.s3_bucket}/{s3_key}"
        s3_console_url = f"https://s3.console.aws.amazon.com/s3/object/{config.s3_bucket}?prefix={s3_key}"

        print(f"  âœ“ Stored raw JSON: {s3_uri}")

        # Create link artifact (clickable in Prefect UI)
        create_link_artifact(
            key=f"raw-stock-{str.lower(symbol)}-{now.strftime('%Y%m%d-%H%M%S')}",
            link=s3_console_url,
            description=f"ðŸ“„ Raw JSON stock data for {symbol} at ${data.price:.2f}"
        )

        # Generate asset ID
        asset_id = f"s3-raw-{symbol}-{now.strftime('%Y%m%d%H%M%S')}"

        # Emit asset creation event for Prefect Cloud
        emit_event(
            event="prefect.asset.created",
            resource={
                "prefect.resource.id": asset_id,
                "prefect.resource.name": f"Raw Stock Data: {symbol}",
                "prefect.resource.role": "asset"
            },
            payload={
                "uri": s3_uri,
                "symbol": symbol,
                "format": "json",
                "size_bytes": len(json_data),
                "partition_date": now.strftime('%Y-%m-%d'),
                "partition_hour": now.strftime('%H'),
                "price": data.price,
                "volume": data.volume
            }
        )

        # Return metadata for downstream tasks
        return AssetMetadata(
            symbol=symbol,
            uri=s3_uri,
            s3_key=s3_key,
            asset_id=asset_id,
            timestamp=now.isoformat(),
            console_url=s3_console_url,
            format="json",
            size_bytes=len(json_data)
        )

    except Exception as e:
        print(f"  âŒ Failed to store {symbol} to S3: {e}")
        raise


@materialize(
    "s3://s3bucket/stocks/xxx",
    log_prints=True,
    tags=["processing", "parquet", "asset"]
)
def process_to_parquet(
    data: StockData,
    raw_metadata: AssetMetadata
) -> AssetMetadata:
    """
    Process raw data to optimized Parquet format with derived fields

    Processing:
    - Adds derived fields (day_of_week, price_change_pct)
    - Converts to columnar Parquet format
    - Snappy compression
    - Partitioned by year/month/day
    - Tracks lineage to upstream raw asset

    Storage Structure:
    s3://bucket/processed/stocks/year=YYYY/month=MM/day=DD/SYMBOL_HHMMSS.parquet

    Args:
        data: Raw stock data
        raw_metadata: Metadata from upstream raw storage task

    Returns:
        AssetMetadata: Processed asset metadata with lineage
    """

    symbol = data.symbol
    now = datetime.now()

    print(f"  ðŸ”„ Processing {symbol} to Parquet...")

    # Calculate derived fields
    price_change_pct = (
        ((data.price - data.previous_close) / data.previous_close * 100)
        if data.previous_close else 0.0
    )

    # Create enriched dataframe
    df = pd.DataFrame([{
        'symbol': data.symbol,
        'timestamp': pd.to_datetime(data.timestamp),
        'price': data.price,
        'volume': data.volume,
        'open': data.open,
        'high': data.high,
        'low': data.low,
        'previous_close': data.previous_close,
        'change': data.change,
        # Derived fields
        'price_change_pct': round(price_change_pct, 4),
        'ingestion_date': now.date(),
        'ingestion_hour': now.hour,
        'day_of_week': now.strftime('%A'),
        'pipeline_version': config.pipeline_version,
        'processed_at': now
    }])

    # Create partitioned S3 key
    s3_key = (
        f"processed/stocks/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{symbol}_{now.strftime('%H%M%S')}.parquet"
    )

    try:
        # Convert to Parquet with compression
        parquet_buffer = io.BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False,
            coerce_timestamps='us',
            allow_truncated_timestamps=True
        )
        parquet_buffer.seek(0)

        # Upload to S3
        s3.put_object(
            Bucket=config.s3_bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream',
            Metadata={
                'symbol': symbol,
                'timestamp': now.isoformat(),
                'pipeline': 'market-data',
                'format': 'parquet',
                'compression': 'snappy',
                'version': config.pipeline_version,
                'upstream_asset_id': raw_metadata.asset_id
            }
        )

        s3_uri = f"s3://{config.s3_bucket}/{s3_key}"
        s3_console_url = f"https://s3.console.aws.amazon.com/s3/object/{config.s3_bucket}?prefix={s3_key}"

        print(f"  âœ“ Processed to Parquet: {s3_uri}")

        # Create link artifact
        create_link_artifact(
            key=f"processed-stock-{str.lower(symbol)}-{now.strftime('%Y%m%d-%H%M%S')}",
            link=s3_console_url,
            description=f"ðŸ“¦ Processed Parquet for {symbol} (compressed)"
        )

        # Generate asset ID
        asset_id = f"s3-processed-{symbol}-{now.strftime('%Y%m%d%H%M%S')}"

        # Emit asset creation event with lineage
        emit_event(
            event="prefect.asset.created",
            resource={
                "prefect.resource.id": asset_id,
                "prefect.resource.name": f"Processed Stock Data: {symbol}",
                "prefect.resource.role": "asset"
            },
            payload={
                "uri": s3_uri,
                "symbol": symbol,
                "format": "parquet",
                "compression": "snappy",
                "size_bytes": len(parquet_buffer.getvalue()),
                "partition_year": now.year,
                "partition_month": now.month,
                "partition_day": now.day,
                "upstream_asset_id": raw_metadata.asset_id,
                "upstream_uri": raw_metadata.uri,
                "price_change_pct": round(price_change_pct, 2)
            },
            related=[{
                "prefect.resource.id": raw_metadata.asset_id,
                "prefect.resource.role": "asset"
            }]
        )

        return AssetMetadata(
            symbol=symbol,
            uri=s3_uri,
            s3_key=s3_key,
            asset_id=asset_id,
            timestamp=now.isoformat(),
            console_url=s3_console_url,
            format="parquet",
            size_bytes=len(parquet_buffer.getvalue()),
            upstream_asset_id=raw_metadata.asset_id
        )

    except Exception as e:
        print(f"  âŒ Failed to process {symbol} to Parquet: {e}")
        raise


# ============================================================================
# Orchestration Flow
# ============================================================================

@flow(
    name="market-data-pipeline",
    description="Production market data pipeline with full asset tracking",
    log_prints=True,
    retries=1,
    retry_delay_seconds=30
)
def market_data_pipeline(symbols: Optional[List[str]] = None):
    """
    Main orchestration flow for market data ingestion

    Pipeline Steps:
    1. Fetch data from Alpha Vantage API
    2. Validate data quality
    3. Store raw JSON to S3 (with asset tracking)
    4. Process to Parquet (with lineage)
    5. Create summary artifact

    Features:
    - Parallel execution per symbol
    - Transaction safety
    - Full asset lineage
    - Comprehensive error handling
    - Summary reporting

    Args:
        symbols: List of stock symbols (optional, uses config default)
    """

    if symbols is None:
        symbols = config.symbols

    start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"ðŸš€ Starting Market Data Pipeline")
    print(f"{'='*60}")
    print(f"ðŸ• Timestamp: {start_time.isoformat()}")
    print(f"ðŸ“Š Symbols: {', '.join(symbols)}")
    print(f"ðŸ·ï¸  Version: {config.pipeline_version}")
    print(f"{'='*60}\n")

    # Track results
    results = {
        'successful': [],
        'failed': [],
        'raw_assets': [],
        'processed_assets': []
    }

    # Process each symbol
    for symbol in symbols:
        try:
            print(f"\n--- Processing {symbol} ---")

            # Step 1: Fetch data
            stock_data = fetch_stock_price(symbol)

            # Step 2: Validate
            validated_data = validate_data(stock_data)

            # Step 3: Store raw (with transaction and asset tracking)
            with transaction():
                raw_asset_key = f"s3://{config.s3_bucket}/stocks/{str.lower(symbol)}"
                raw_asset = store_to_s3_raw.with_options(
                    assets=[raw_asset_key],
                    log_prints=True,
                    tags=["storage", "s3", "raw", "asset"]
                )(validated_data)
                results['raw_assets'].append(raw_asset)

            # Step 4: Process to parquet (with transaction, asset tracking, and lineage)
            with transaction():
                processed_asset_key = f"s3://{config.s3_bucket}/processed/stocks/{str.lower(symbol)}"
                processed_asset = process_to_parquet.with_options(
                    assets=[processed_asset_key],
                    asset_deps=[raw_asset_key],
                    log_prints=True,
                    tags=["processing", "parquet", "asset"]
                )(validated_data, raw_asset)
                results['processed_assets'].append(processed_asset)

            results['successful'].append(symbol)
            print(f"âœ… {symbol} completed successfully\n")

        except Exception as e:
            print(f"âŒ {symbol} failed: {e}\n")
            results['failed'].append({'symbol': symbol, 'error': str(e)})

    # Calculate execution time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Create summary markdown artifact
    success_count = len(results['successful'])
    fail_count = len(results['failed'])
    total_count = len(symbols)

    lineage_diagram = _create_lineage_diagram(results)

    summary = f"""# ðŸ“Š Market Data Pipeline Summary

## Execution Overview
- **Run Time**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Duration**: {duration:.2f} seconds
- **Pipeline Version**: {config.pipeline_version}

## Results
- **Total Symbols**: {total_count}
- **âœ… Successful**: {success_count}
- **âŒ Failed**: {fail_count}
- **Success Rate**: {(success_count/total_count*100):.1f}%

## Processed Symbols
{', '.join(results['successful']) if results['successful'] else 'None'}

## Failed Symbols
{', '.join([f"{f['symbol']} ({f['error']})" for f in results['failed']]) if results['failed'] else 'None'}

## Asset Lineage
{lineage_diagram}

## S3 Storage
- **Bucket**: `{config.s3_bucket}`
- **Raw Assets**: {len(results['raw_assets'])} files
- **Processed Assets**: {len(results['processed_assets'])} files

## Next Steps
- Query data with AWS Athena
- View lineage in Prefect Cloud Events
- Set up DataHub for enterprise catalog
"""

    create_markdown_artifact(
        key=f"pipeline-summary-{start_time.strftime('%Y%m%d-%H%M%S')}",
        markdown=summary,
        description="Pipeline execution summary with lineage"
    )

    # Emit completion event
    emit_event(
        event="pipeline.completed",
        resource={
            "prefect.resource.id": "market-data-pipeline",
            "prefect.resource.name": "Market Data Pipeline"
        },
        payload={
            "duration_seconds": duration,
            "symbols_processed": success_count,
            "symbols_failed": fail_count,
            "total_assets_created": len(results['raw_assets']) + len(results['processed_assets'])
        }
    )

    print(f"\n{'='*60}")
    print(f"âœ¨ Pipeline Complete!")
    print(f"{'='*60}")
    print(f"â±ï¸  Duration: {duration:.2f}s")
    print(f"âœ… Success: {success_count}/{total_count}")
    print(f"ðŸ“¦ Assets Created: {len(results['raw_assets']) + len(results['processed_assets'])}")
    print(f"{'='*60}\n")

    return results


def _create_lineage_diagram(results: dict) -> str:
    """Create ASCII lineage diagram"""
    if not results['successful']:
        return "_No successful assets to display_"

    diagram = "```\n"
    for symbol in results['successful']:
        diagram += f"""
{symbol} Data Flow:
  Alpha Vantage API
       â†“ (fetch)
  s3://.../stocks/.../{symbol}.json (raw JSON)
       â†“ (transform + enrich)
  s3://.../processed/.../{symbol}.parquet (processed Parquet)
       â†“ (query)
  AWS Athena Table
"""
    diagram += "```"
    return diagram


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    # Run the pipeline
    market_data_pipeline()
