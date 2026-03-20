"""
Market Data Pipeline with Spark Analytics on AWS Glue (Airflow DAG)
Equivalent of flows/market_data_with_glue.py

Pipeline:
  1. Fetch stock data from Alpha Vantage
  2. Validate, store raw JSON, process to Parquet
  3. Run Spark analytics on AWS Glue (serverless)
  4. Detect buy signals and send email alerts
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import os

from helpers import (
    get_symbols,
    fetch_stock_price,
    validate_data,
    store_to_s3_raw,
    process_to_parquet,
    detect_buy_signals,
    send_email_alerts,
)

# ============================================================================
# DAG Configuration
# ============================================================================

S3_BUCKET = Variable.get("S3_BUCKET", default_var=os.getenv("S3_BUCKET", ""))
ALPHA_VANTAGE_KEY = Variable.get(
    "ALPHA_VANTAGE_API_KEY", default_var=os.getenv("ALPHA_VANTAGE_API_KEY", "")
)
AWS_REGION = Variable.get("AWS_REGION", default_var=os.getenv("AWS_REGION", "us-east-1"))
GLUE_ROLE = "AWSGlueServiceRole-MarketData"

default_args = {
    "owner": "market-data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# ============================================================================
# Task Callables
# ============================================================================

def collect_market_data(**context):
    """Fetch, validate, and store market data for all symbols."""
    symbols = get_symbols()
    results = {"successful": [], "failed": []}

    print(f"[INFO] Processing {len(symbols)} symbols: {', '.join(symbols)}")

    for symbol in symbols:
        try:
            data = fetch_stock_price(symbol, ALPHA_VANTAGE_KEY)
            validated = validate_data(data)
            store_to_s3_raw(validated, S3_BUCKET)
            process_to_parquet(validated, S3_BUCKET)
            results["successful"].append(symbol)
            print(f"[OK] {symbol} completed")
        except Exception as e:
            print(f"[ERROR] {symbol} failed: {e}")
            results["failed"].append({"symbol": symbol, "error": str(e)})

    success = len(results["successful"])
    failed = len(results["failed"])
    print(f"\n[COMPLETE] Data collection: {success} succeeded, {failed} failed")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="collection_results", value=results)

    if success == 0:
        raise ValueError("No data collected for any symbol")

    return results


def check_buy_signals(**context):
    """Detect buy signals from ML features and send email alerts."""
    print("[INFO] Running buy signal detection...")
    signals = detect_buy_signals(S3_BUCKET)
    if signals:
        send_email_alerts(signals)
    context["ti"].xcom_push(key="signal_count", value=len(signals))
    return signals


def print_summary(**context):
    """Print pipeline summary."""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="collect_market_data", key="collection_results") or {}
    signal_count = ti.xcom_pull(task_ids="buy_signal_alerts", key="signal_count") or 0

    success = len(results.get("successful", []))

    print("\n" + "=" * 70)
    print("[COMPLETE] PIPELINE SUMMARY")
    print("=" * 70)
    print(f"[OK] Data Collection: {success} symbols")
    print(f"[RUN] Spark Analytics: 3 Glue jobs")
    print(f"[SIGNALS] Buy Signals: {signal_count} detected")
    print("=" * 70)


# ============================================================================
# DAG Definition
# ============================================================================

with DAG(
    dag_id="market_data_pipeline_glue",
    default_args=default_args,
    description="Market data collection + Spark analytics on AWS Glue (serverless)",
    schedule="0 18 * * *",  # 6 PM ET daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["market-data", "glue", "spark", "production"],
) as dag:

    # Phase 1: Data Collection
    collect = PythonOperator(
        task_id="collect_market_data",
        python_callable=collect_market_data,
    )

    # Phase 2: Spark Analytics on Glue (3 jobs in sequence)
    run_daily_analytics = GlueJobOperator(
        task_id="glue_daily_analytics",
        job_name="market-data-daily-analytics",
        region_name=AWS_REGION,
        num_of_dpus=3,
        script_args={"--S3_BUCKET": S3_BUCKET},
        wait_for_completion=True,
        verbose=True,
    )

    run_ml_features = GlueJobOperator(
        task_id="glue_ml_features",
        job_name="market-data-ml-features",
        region_name=AWS_REGION,
        num_of_dpus=3,
        script_args={"--S3_BUCKET": S3_BUCKET},
        wait_for_completion=True,
        verbose=True,
    )

    run_volatility = GlueJobOperator(
        task_id="glue_volatility_metrics",
        job_name="market-data-volatility",
        region_name=AWS_REGION,
        num_of_dpus=3,
        script_args={"--S3_BUCKET": S3_BUCKET},
        wait_for_completion=True,
        verbose=True,
    )

    # Phase 3: Buy Signal Alerts (after ML features are ready)
    alerts = PythonOperator(
        task_id="buy_signal_alerts",
        python_callable=check_buy_signals,
    )

    # Phase 4: Summary
    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=print_summary,
        trigger_rule="all_done",
    )

    # Task dependencies
    # Data collection first, then Glue jobs in sequence, then alerts, then summary
    collect >> run_daily_analytics >> run_ml_features >> run_volatility >> alerts >> summary
