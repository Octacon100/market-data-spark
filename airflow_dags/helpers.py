"""
Shared helpers for Airflow DAGs.
Contains data models, API fetching, validation, S3 storage, and buy signal detection.
Reusable across all DAGs without any Airflow or Prefect dependencies.
"""

import boto3
import requests
import pandas as pd
import io
import os
import json
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path


# ============================================================================
# Configuration
# ============================================================================

WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "watchlist.json"
SETTINGS_PATH = Path(__file__).parent.parent / "config" / "pipeline_settings.json"


def load_watchlist() -> list:
    """Load stock symbols from config/watchlist.json (plain list of tickers)"""
    if WATCHLIST_PATH.exists():
        with open(WATCHLIST_PATH, "r") as f:
            return json.load(f)
    return []


def load_settings() -> dict:
    """Load pipeline settings from config/pipeline_settings.json"""
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, "r") as f:
            return json.load(f)
    return {}


def get_symbols() -> List[str]:
    """Get stock symbols with priority: SYMBOLS env var > watchlist.json > default"""
    env_symbols = os.getenv("SYMBOLS", "")
    if env_symbols:
        return [s.strip() for s in env_symbols.split(",") if s.strip()]
    watchlist = load_watchlist()
    if watchlist:
        return watchlist
    return ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]


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


# ============================================================================
# Data Fetching
# ============================================================================

def fetch_stock_price(symbol: str, api_key: str) -> StockData:
    """
    Fetch real-time stock price from Alpha Vantage API.

    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL')
        api_key: Alpha Vantage API key

    Returns:
        StockData: Structured stock data

    Raises:
        ValueError: If API returns error or invalid data
    """
    print(f"[INFO] Fetching {symbol} from Alpha Vantage...")

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": api_key,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
        raise ValueError(f"API Error for {symbol}: {data['Error Message']}")
    if "Note" in data:
        raise ValueError(f"Rate limit hit for {symbol}")

    quote = data.get("Global Quote", {})
    if not quote:
        raise ValueError(f"No quote data returned for {symbol}")

    stock_data = StockData(
        symbol=symbol,
        timestamp=datetime.now().isoformat(),
        price=float(quote.get("05. price", 0)),
        volume=int(quote.get("06. volume", 0)),
        open=float(quote.get("02. open", 0)),
        high=float(quote.get("03. high", 0)),
        low=float(quote.get("04. low", 0)),
        previous_close=float(quote.get("08. previous close", 0)),
        change=float(quote.get("09. change", 0)),
        change_percent=quote.get("10. change percent", "0%"),
    )

    print(f"  [OK] {symbol}: ${stock_data.price:.2f} ({stock_data.change_percent})")
    return stock_data


# ============================================================================
# Data Validation
# ============================================================================

def validate_data(data: StockData) -> StockData:
    """
    Validate stock data quality.

    Raises:
        ValueError: If price <= 0 or volume < 0
    """
    if data.price <= 0:
        raise ValueError(f"Invalid price for {data.symbol}: ${data.price}")
    if data.volume < 0:
        raise ValueError(f"Invalid volume for {data.symbol}: {data.volume}")

    if data.previous_close and data.price:
        change_pct = abs((data.price - data.previous_close) / data.previous_close)
        if change_pct > 0.20:
            print(f"  [WARN] Large price movement for {data.symbol}: {change_pct*100:.1f}%")

    print(f"  [OK] Validation passed for {data.symbol}")
    return data


# ============================================================================
# S3 Storage
# ============================================================================

def store_to_s3_raw(data: StockData, bucket: str) -> dict:
    """
    Store raw JSON to S3 with date/hour partitioning.

    Returns:
        dict with s3_key and s3_uri
    """
    s3 = boto3.client("s3")
    now = datetime.now()

    s3_key = (
        f"stocks/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"hour={now.strftime('%H')}/"
        f"{data.symbol}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    json_data = json.dumps(data.to_dict(), indent=2)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_data,
        ContentType="application/json",
        Metadata={
            "symbol": data.symbol,
            "timestamp": now.isoformat(),
            "pipeline": "market-data",
            "format": "json",
        },
    )

    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"  [OK] Stored raw JSON: {s3_uri}")

    return {"s3_key": s3_key, "s3_uri": s3_uri, "size_bytes": len(json_data)}


def process_to_parquet(data: StockData, bucket: str) -> dict:
    """
    Process raw data to Parquet with derived fields and year/month/day partitioning.

    Returns:
        dict with s3_key and s3_uri
    """
    s3 = boto3.client("s3")
    now = datetime.now()

    price_change_pct = (
        ((data.price - data.previous_close) / data.previous_close * 100)
        if data.previous_close
        else 0.0
    )

    df = pd.DataFrame(
        [
            {
                "symbol": data.symbol,
                "timestamp": pd.to_datetime(data.timestamp),
                "price": data.price,
                "volume": data.volume,
                "open": data.open,
                "high": data.high,
                "low": data.low,
                "previous_close": data.previous_close,
                "change": data.change,
                "price_change_pct": round(price_change_pct, 4),
                "ingestion_date": now.date(),
                "ingestion_hour": now.hour,
                "day_of_week": now.strftime("%A"),
                "processed_at": now,
            }
        ]
    )

    s3_key = (
        f"processed/stocks/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{data.symbol}_{now.strftime('%H%M%S')}.parquet"
    )

    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        compression="snappy",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    parquet_buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
        Metadata={
            "symbol": data.symbol,
            "timestamp": now.isoformat(),
            "pipeline": "market-data",
            "format": "parquet",
            "compression": "snappy",
        },
    )

    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"  [OK] Processed to Parquet: {s3_uri}")

    return {
        "s3_key": s3_key,
        "s3_uri": s3_uri,
        "size_bytes": len(parquet_buffer.getvalue()),
    }


# ============================================================================
# Buy Signal Detection
# ============================================================================

def detect_buy_signals(bucket: str) -> list:
    """
    Read ML features from S3 and detect buy signals based on pipeline_settings.json rules.

    Returns:
        list of signal dicts
    """
    settings = load_settings()
    rules = settings.get("buy_signals", {}).get("rules", {})

    if not settings.get("buy_signals", {}).get("enabled", False):
        print("[INFO] Buy signal detection is disabled in pipeline_settings.json")
        return []

    s3 = boto3.client("s3")
    input_prefix = "analytics/ml_features/"
    print(f"[INFO] Reading ML features from s3://{bucket}/{input_prefix}")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        parquet_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=input_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_keys.append(obj["Key"])

        if not parquet_keys:
            print("[WARN] No ML features data found in S3 - run Spark analytics first")
            return []

        frames = []
        for key in parquet_keys:
            response = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(response["Body"].read())
            frames.append(pd.read_parquet(buf))

        df = pd.concat(frames, ignore_index=True)
        print(f"[OK] Loaded {len(df)} records from ML features")
    except Exception as e:
        print(f"[ERROR] Failed to read ML features: {e}")
        return []

    if df.empty:
        print("[WARN] ML features DataFrame is empty")
        return []

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    latest = df.sort_values("timestamp").groupby("symbol").tail(2)

    signals = []

    for symbol in latest["symbol"].unique():
        sym_data = latest[latest["symbol"] == symbol].sort_values("timestamp")
        if len(sym_data) < 2:
            continue

        prev = sym_data.iloc[-2]
        curr = sym_data.iloc[-1]

        if rules.get("golden_cross", {}).get("enabled", False):
            if prev.get("ma_7d", 0) <= prev.get("ma_30d", 0) and curr.get("ma_7d", 0) > curr.get("ma_30d", 0):
                signals.append({
                    "symbol": symbol, "signal": "Golden Cross",
                    "description": "7-day MA crossed above 30-day MA",
                    "price": float(curr.get("price", 0)),
                    "ma_7d": float(curr.get("ma_7d", 0)),
                    "ma_30d": float(curr.get("ma_30d", 0)),
                    "timestamp": str(curr["timestamp"]), "strength": "strong",
                })

        min_momentum = rules.get("momentum_surge", {}).get("min_momentum_pct", 3.0)
        if rules.get("momentum_surge", {}).get("enabled", False):
            momentum = curr.get("price_momentum_7d", 0)
            if momentum and momentum >= min_momentum:
                signals.append({
                    "symbol": symbol, "signal": "Momentum Surge",
                    "description": f"7-day momentum at {momentum:.1f}% (threshold: {min_momentum}%)",
                    "price": float(curr.get("price", 0)),
                    "momentum_7d": float(momentum),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "strong" if momentum >= min_momentum * 2 else "moderate",
                })

        if rules.get("oversold_bounce", {}).get("enabled", False):
            if prev.get("price", 0) < prev.get("ma_30d", 0) and curr.get("price", 0) > curr.get("ma_30d", 0):
                signals.append({
                    "symbol": symbol, "signal": "Oversold Bounce",
                    "description": "Price recovered above 30-day MA",
                    "price": float(curr.get("price", 0)),
                    "ma_30d": float(curr.get("ma_30d", 0)),
                    "timestamp": str(curr["timestamp"]), "strength": "moderate",
                })

        max_vol = rules.get("low_vol_uptrend", {}).get("max_volatility_7d_pct", 2.0)
        min_mom = rules.get("low_vol_uptrend", {}).get("min_momentum_pct", 1.0)
        if rules.get("low_vol_uptrend", {}).get("enabled", False):
            vol_7d = curr.get("volatility_7d", None)
            momentum = curr.get("price_momentum_7d", 0)
            if vol_7d is not None and vol_7d < max_vol and momentum and momentum >= min_mom:
                signals.append({
                    "symbol": symbol, "signal": "Low Vol Uptrend",
                    "description": f"Stable uptrend (vol: {vol_7d:.2f}, momentum: {momentum:.1f}%)",
                    "price": float(curr.get("price", 0)),
                    "volatility_7d": float(vol_7d),
                    "momentum_7d": float(momentum),
                    "timestamp": str(curr["timestamp"]), "strength": "moderate",
                })

    if signals:
        print(f"[OK] Detected {len(signals)} buy signal(s):")
        for sig in signals:
            print(f"  - {sig['symbol']}: {sig['signal']} ({sig['description']})")
    else:
        print("[INFO] No buy signals detected")

    return signals


# ============================================================================
# Email Alerts
# ============================================================================

def send_email_alerts(signals: list):
    """Send buy signal alerts via AWS SES (if configured in pipeline_settings.json)."""
    if not signals:
        print("[INFO] No signals to send")
        return

    settings = load_settings()
    alert_config = settings.get("alerts", {})

    if not alert_config.get("email_enabled", False):
        print("[INFO] Email alerts disabled in pipeline_settings.json")
        return

    recipients = alert_config.get("recipients", [])
    if not recipients:
        print("[WARN] No email recipients configured in pipeline_settings.json")
        return

    sender = os.getenv("ALERT_EMAIL_SENDER", "")
    if not sender:
        print("[WARN] ALERT_EMAIL_SENDER not set - skipping email")
        return

    region = os.getenv("AWS_REGION", "us-east-1")
    ses = boto3.client("ses", region_name=region)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[Market Data] {len(signals)} Buy Signal(s) Detected - {now}"

    text_lines = [
        "Market Data Pipeline - Buy Signal Alert",
        f"Generated: {now}",
        f"Signals Detected: {len(signals)}",
        "",
        "=" * 50,
    ]
    for sig in signals:
        text_lines.extend([
            "",
            f"  Symbol: {sig['symbol']}",
            f"  Signal: {sig['signal']}",
            f"  Detail: {sig['description']}",
            f"  Price:  ${sig['price']:.2f}",
            f"  Strength: {sig['strength']}",
            f"  Time:  {sig['timestamp']}",
            "-" * 50,
        ])
    text_lines.extend([
        "",
        "NOTE: These signals are for informational purposes only.",
        "They are not financial advice. Always do your own research.",
    ])
    text_body = "\n".join(text_lines)

    signal_rows = ""
    for sig in signals:
        color = "#2e7d32" if sig["strength"] == "strong" else "#f57f17"
        signal_rows += (
            f'<tr>'
            f'<td style="padding:8px;border:1px solid #ddd;font-weight:bold">{sig["symbol"]}</td>'
            f'<td style="padding:8px;border:1px solid #ddd">{sig["signal"]}</td>'
            f'<td style="padding:8px;border:1px solid #ddd">{sig["description"]}</td>'
            f'<td style="padding:8px;border:1px solid #ddd">${sig["price"]:.2f}</td>'
            f'<td style="padding:8px;border:1px solid #ddd;color:{color}">{sig["strength"]}</td>'
            f'</tr>'
        )

    html_body = (
        '<html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto">'
        '<h2 style="color:#1565c0">Market Data Pipeline - Buy Signal Alert</h2>'
        f'<p>Generated: {now} | Signals: {len(signals)}</p>'
        '<table style="border-collapse:collapse;width:100%">'
        '<tr style="background:#1565c0;color:white">'
        '<th style="padding:8px;border:1px solid #ddd">Symbol</th>'
        '<th style="padding:8px;border:1px solid #ddd">Signal</th>'
        '<th style="padding:8px;border:1px solid #ddd">Detail</th>'
        '<th style="padding:8px;border:1px solid #ddd">Price</th>'
        '<th style="padding:8px;border:1px solid #ddd">Strength</th>'
        f'</tr>{signal_rows}</table>'
        '<p style="color:#888;font-size:12px;margin-top:20px">'
        'These signals are for informational purposes only. '
        'They are not financial advice. Always do your own research.'
        '</p></body></html>'
    )

    try:
        ses.send_email(
            Source=sender,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        print(f"[OK] Email sent to {', '.join(recipients)}")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")
        print("[INFO] Check AWS SES setup - sender and recipients must be verified")
