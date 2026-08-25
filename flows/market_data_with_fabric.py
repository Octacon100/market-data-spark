"""
Market Data Pipeline - Microsoft Fabric Edition

Uses OneLake (Azure Data Lake Storage Gen2) instead of S3, and
Microsoft Fabric Spark notebooks instead of AWS Glue jobs.

Storage layout in OneLake (Files/):
  stocks/date=YYYY-MM-DD/hour=HH/<SYMBOL>_<ts>.json   raw JSON
  processed/stocks/<SYMBOL>/part-*.parquet             processed parquet
  analytics/daily_stats/                               Spark output
  analytics/ml_features/                               Spark output
  analytics/volatility_metrics/                        Spark output
  trending/<date>/trending_tickers.json                social scanner output

Prerequisites:
  1. Azure service principal with Storage Blob Data Contributor on the workspace
  2. Fabric workspace with a Lakehouse named 'MarketDataLakehouse'
  3. Three Fabric notebooks created from spark_jobs/fabric/
  4. Environment variables (see .env.example for the full list)
"""

import io
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from config_utils import resolve, WATCHLIST_PATH, SETTINGS_PATH
from daily_digest import daily_digest_flow
from fabric_tasks import fabric_analytics_flow
from notify import on_flow_complete, on_flow_failure
from onelake_storage import OneLakeClient


# ============================================================================
# Data model (shared with market_data_flow.py)
# ============================================================================

@dataclass
class StockData:
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

    def to_dict(self):
        return asdict(self)


# ============================================================================
# Config
# ============================================================================

def load_watchlist() -> list:
    if WATCHLIST_PATH.exists():
        with open(WATCHLIST_PATH) as f:
            return json.load(f)
    return []


def get_symbols() -> List[str]:
    env_symbols = os.getenv("SYMBOLS", "")
    if env_symbols:
        return [s.strip() for s in env_symbols.split(",") if s.strip()]
    watchlist = load_watchlist()
    return watchlist if watchlist else ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]


# ============================================================================
# Fetch (same logic as market_data_flow.py, no storage dependency)
# ============================================================================

@task(retries=3, retry_delay_seconds=60, log_prints=True, tags=["api", "fetch"])
def fetch_stock_price_fabric(symbol: str) -> Optional[StockData]:
    """Fetch real-time price from Alpha Vantage."""
    api_key = resolve("alpha-vantage-api-key", "ALPHA_VANTAGE_API_KEY", is_secret=True)
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY not set")

    print(f"[FETCH] {symbol}")
    resp = requests.get(
        "https://www.alphavantage.co/query",
        params={"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": api_key},
        timeout=(60, 10),
    )
    resp.raise_for_status()
    data = resp.json()

    if "Error Message" in data:
        raise ValueError(f"API error for {symbol}: {data['Error Message']}")
    if "Note" in data:
        raise ValueError(f"Rate limit hit for {symbol}")

    quote = data.get("Global Quote", {})
    if not quote:
        raise ValueError(f"No quote data for {symbol}")

    stock = StockData(
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
    print(f"  [OK] {symbol}: ${stock.price:.2f} ({stock.change_percent})")
    return stock


# ============================================================================
# Validate
# ============================================================================

@task(log_prints=True, tags=["validation"])
def validate_stock_data(data: StockData) -> StockData:
    if data.price <= 0:
        raise ValueError(f"Invalid price for {data.symbol}: ${data.price}")
    if data.volume < 0:
        raise ValueError(f"Invalid volume for {data.symbol}: {data.volume}")
    print(f"  [OK] Validation passed: {data.symbol}")
    return data


# ============================================================================
# Store to OneLake
# ============================================================================

@task(log_prints=True, tags=["storage", "onelake"])
def store_to_onelake(data: StockData) -> str:
    """
    Write raw JSON to OneLake at:
      Files/stocks/date=YYYY-MM-DD/hour=HH/<SYMBOL>_<ts>.json

    Returns the OneLake path written.
    """
    client = OneLakeClient()
    now = datetime.now()
    path = (
        f"stocks/date={now.strftime('%Y-%m-%d')}"
        f"/hour={now.strftime('%H')}"
        f"/{data.symbol}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )
    payload = json.dumps(data.to_dict(), indent=2)
    client.upload_text(path, payload)
    print(f"  [OK] {data.symbol} -> OneLake: Files/{path}")
    return path


@task(log_prints=True, tags=["storage", "onelake"])
def process_to_onelake_parquet(data: StockData) -> str:
    """
    Convert to Parquet and write to:
      Files/processed/stocks/<SYMBOL>/part-<ts>.parquet
    """
    client = OneLakeClient()
    now = datetime.now()
    df = pd.DataFrame([data.to_dict()])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    path = f"processed/stocks/{data.symbol}/part-{now.strftime('%Y%m%d%H%M%S')}.parquet"
    client.upload_bytes(path, buf.read())
    print(f"  [OK] {data.symbol} parquet -> OneLake: Files/{path}")
    return path


# ============================================================================
# Buy signal detection (reads from OneLake instead of S3)
# ============================================================================

@task(log_prints=True, tags=["signals", "onelake"])
def detect_buy_signals_onelake(settings: dict) -> list:
    """
    Read ML features parquet from OneLake and detect buy signals.
    Mirrors the logic in buy_signal_alerts.detect_buy_signals, but reads
    from OneLake instead of S3.
    """
    rules = settings.get("buy_signals", {}).get("rules", {})

    if not settings.get("buy_signals", {}).get("enabled", False):
        print("[INFO] Buy signal detection disabled in pipeline_settings.json")
        return []

    client = OneLakeClient()
    parquet_paths = client.list_files("analytics/ml_features/", suffix=".parquet")

    if not parquet_paths:
        print("[WARN] No ML features data found in OneLake - run Fabric Spark first")
        return []

    frames = []
    for rel_path in parquet_paths:
        raw = client.download_bytes(rel_path)
        df = pd.read_parquet(io.BytesIO(raw))
        for part in rel_path.split("/"):
            if part.startswith("symbol="):
                df["symbol"] = part.split("=", 1)[1]
                break
        frames.append(df)

    if not frames:
        return []

    df_all = pd.concat(frames, ignore_index=True)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
    latest = df_all.sort_values("timestamp").groupby("symbol").tail(2)

    signals = []
    for symbol in latest["symbol"].unique():
        sym = latest[latest["symbol"] == symbol].sort_values("timestamp")
        if len(sym) < 2:
            continue
        prev, curr = sym.iloc[-2], sym.iloc[-1]

        if rules.get("golden_cross", {}).get("enabled", False):
            if prev.get("ma_7d", 0) <= prev.get("ma_30d", 0) and curr.get("ma_7d", 0) > curr.get("ma_30d", 0):
                signals.append({"symbol": symbol, "signal": "Golden Cross",
                    "description": "7d MA crossed above 30d MA",
                    "price": float(curr.get("price", 0)),
                    "timestamp": str(curr["timestamp"]), "strength": "strong"})

        min_mom = rules.get("momentum_surge", {}).get("min_momentum_pct", 3.0)
        if rules.get("momentum_surge", {}).get("enabled", False):
            momentum = curr.get("price_momentum_7d", 0)
            if momentum and momentum >= min_mom:
                signals.append({"symbol": symbol, "signal": "Momentum Surge",
                    "description": f"7d momentum {momentum:.1f}% (threshold {min_mom}%)",
                    "price": float(curr.get("price", 0)),
                    "momentum_7d": float(momentum),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "strong" if momentum >= min_mom * 2 else "moderate"})

        if rules.get("oversold_bounce", {}).get("enabled", False):
            if prev.get("price", 0) < prev.get("ma_30d", 0) and curr.get("price", 0) > curr.get("ma_30d", 0):
                signals.append({"symbol": symbol, "signal": "Oversold Bounce",
                    "description": "Price recovered above 30d MA",
                    "price": float(curr.get("price", 0)),
                    "timestamp": str(curr["timestamp"]), "strength": "moderate"})

    if signals:
        print(f"[OK] Detected {len(signals)} buy signal(s)")
        for s in signals:
            print(f"  - {s['symbol']}: {s['signal']}")
    else:
        print("[INFO] No buy signals detected")
    return signals


# ============================================================================
# Main pipeline flow
# ============================================================================

@flow(
    name="market-data-pipeline-fabric",
    description="Market data pipeline using OneLake storage + Microsoft Fabric Spark",
    log_prints=True,
    on_completion=[on_flow_complete],
    on_failure=[on_flow_failure],
)
def market_data_pipeline_with_fabric():
    """
    End-to-end pipeline on Microsoft Fabric:
      1. Fetch stock prices from Alpha Vantage
      2. Validate and store to OneLake (raw JSON + Parquet)
      3. Trigger Fabric Spark notebooks for analytics
      4. Detect buy signals from ML features in OneLake
      5. Send daily digest email

    Required env vars / Prefect Variables:
      ALPHA_VANTAGE_API_KEY, FABRIC_WORKSPACE_ID, FABRIC_LAKEHOUSE_NAME,
      AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET,
      FABRIC_NOTEBOOK_DAILY_ANALYTICS, FABRIC_NOTEBOOK_ML_FEATURES,
      FABRIC_NOTEBOOK_VOLATILITY
    """
    print("\n" + "=" * 70)
    print("[START] Market Data Pipeline with Microsoft Fabric")
    print("=" * 70)
    print(f"   Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    workspace_id = resolve("fabric-workspace-id", "FABRIC_WORKSPACE_ID")
    lakehouse = resolve("fabric-lakehouse-name", "FABRIC_LAKEHOUSE_NAME") or "MarketDataLakehouse"
    print(f"   Workspace: {workspace_id}")
    print(f"   Lakehouse: {lakehouse}")
    print("=" * 70 + "\n")

    symbols = get_symbols()
    print(f"[DATA] PHASE 1: Market Data Collection ({len(symbols)} symbols)")
    print("-" * 70)

    successful = []
    failed = []

    for symbol in symbols:
        try:
            raw = fetch_stock_price_fabric(symbol)
            validated = validate_stock_data(raw)
            store_to_onelake(validated)
            process_to_onelake_parquet(validated)
            successful.append(symbol)
        except Exception as e:
            print(f"  [ERROR] {symbol}: {e}")
            failed.append(symbol)

    print(f"\n[OK] Collection: {len(successful)} ok, {len(failed)} failed")

    if not successful:
        print("[WARNING] No data collected - skipping Fabric analytics")
        return {
            "phase": "data_collection_only",
            "successful": successful,
            "failed": failed,
            "fabric_results": None,
            "message": "No data collected, Fabric not triggered",
        }

    # Phase 2: Fabric Spark Analytics
    print("\n" + "=" * 70)
    print("[RUN] PHASE 2: Fabric Spark Analytics")
    print("-" * 70)

    fabric_results = fabric_analytics_flow()

    # Phase 3: Buy Signals
    print("\n" + "=" * 70)
    print("[SIGNALS] PHASE 3: Buy Signal Detection")
    print("-" * 70)

    settings = {}
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH) as f:
            settings = json.load(f)

    signals = detect_buy_signals_onelake(settings)
    signal_count = len(signals)

    # Phase 4: Daily Digest
    print("\n" + "=" * 70)
    print("[DIGEST] PHASE 4: Daily Digest")
    print("-" * 70)

    digest_results = daily_digest_flow(bucket=None)
    digest_count = digest_results.get("trending_count", 0) if isinstance(digest_results, dict) else 0

    # Summary
    print("\n" + "=" * 70)
    print("[COMPLETE] PIPELINE SUMMARY")
    print("=" * 70)
    print(f"[OK] Data Collection: {len(successful)} symbols")
    nb_count = len(fabric_results.get("job_results", []))
    nb_ok = len([j for j in fabric_results.get("job_results", []) if j.get("status") == "success"])
    print(f"[RUN] Fabric Notebooks: {nb_ok}/{nb_count} succeeded")
    print(f"[SIGNALS] Buy Signals : {signal_count} detected")
    print(f"[DIGEST] Trending     : {digest_count}")
    est_cost = fabric_results.get("estimated_cost", 0)
    print(f"[COST] Estimated Cost : ${est_cost:.2f}")
    print("=" * 70 + "\n")

    return {
        "phase": "complete",
        "successful": successful,
        "failed": failed,
        "fabric_results": fabric_results,
        "signals": signals,
        "digest_results": digest_results,
        "summary": {
            "symbols_processed": len(successful),
            "fabric_notebooks_run": nb_count,
            "fabric_notebooks_succeeded": nb_ok,
            "buy_signals_detected": signal_count,
            "digest_trending_count": digest_count,
            "estimated_cost": est_cost,
        },
    }


if __name__ == "__main__":
    market_data_pipeline_with_fabric()
