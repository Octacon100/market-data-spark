"""
Market Data Pipeline with Spark Analytics on EMR (Airflow DAG)
Equivalent of flows/market_data_with_spark.py

Pipeline:
  1. Fetch stock data from Alpha Vantage
  2. Validate, store raw JSON, process to Parquet
  3. Create transient EMR cluster with Spark steps (auto-terminates)
  4. Detect buy signals and send email alerts
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
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
EMR_KEY_PAIR = Variable.get("EMR_KEY_PAIR", default_var=os.getenv("EMR_KEY_PAIR", "your-key-pair"))
EMR_SUBNET_ID = Variable.get("EMR_SUBNET_ID", default_var=os.getenv("EMR_SUBNET_ID", ""))
EMR_RELEASE_LABEL = Variable.get(
    "EMR_RELEASE_LABEL", default_var=os.getenv("EMR_RELEASE_LABEL", "emr-7.3.0")
)
USE_SPOT = Variable.get("USE_SPOT_INSTANCES", default_var=os.getenv("USE_SPOT_INSTANCES", "True")).lower() == "true"
SPOT_BID_PRICE = Variable.get("SPOT_BID_PRICE", default_var=os.getenv("SPOT_BID_PRICE", "0.15"))

default_args = {
    "owner": "market-data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================================
# EMR Cluster Configuration
# ============================================================================

CORE_MARKET = "SPOT" if USE_SPOT else "ON_DEMAND"
CORE_GROUP = {
    "Name": "Core",
    "Market": CORE_MARKET,
    "InstanceRole": "CORE",
    "InstanceType": "m5.xlarge",
    "InstanceCount": 2,
}
if USE_SPOT:
    CORE_GROUP["BidPrice"] = SPOT_BID_PRICE

JOB_FLOW_OVERRIDES = {
    "Name": "MarketData-Airflow-{{ ds_nodash }}",
    "ReleaseLabel": EMR_RELEASE_LABEL,
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            CORE_GROUP,
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2KeyName": EMR_KEY_PAIR,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
    "VisibleToAllUsers": True,
    "Tags": [
        {"Key": "Project", "Value": "MarketDataPipeline"},
        {"Key": "ManagedBy", "Value": "Airflow"},
        {"Key": "Environment", "Value": "Production"},
    ],
}

# Add subnet if configured
if EMR_SUBNET_ID:
    JOB_FLOW_OVERRIDES["Instances"]["Ec2SubnetId"] = EMR_SUBNET_ID

SPARK_STEPS = [
    {
        "Name": "DailyAnalytics",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.yarn.submit.waitAppCompletion=true",
                f"s3://{S3_BUCKET}/scripts/spark/daily_analytics.py",
                "--bucket", S3_BUCKET,
            ],
        },
    },
    {
        "Name": "MLFeatures",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.yarn.submit.waitAppCompletion=true",
                f"s3://{S3_BUCKET}/scripts/spark/ml_features.py",
                "--bucket", S3_BUCKET,
            ],
        },
    },
    {
        "Name": "VolatilityMetrics",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.yarn.submit.waitAppCompletion=true",
                f"s3://{S3_BUCKET}/scripts/spark/volatility_metrics.py",
                "--bucket", S3_BUCKET,
            ],
        },
    },
]


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
    cluster_id = ti.xcom_pull(task_ids="create_emr_cluster", key="return_value") or "N/A"
    signal_count = ti.xcom_pull(task_ids="buy_signal_alerts", key="signal_count") or 0

    success = len(results.get("successful", []))

    print("\n" + "=" * 70)
    print("[COMPLETE] PIPELINE SUMMARY")
    print("=" * 70)
    print(f"[OK] Data Collection: {success} symbols")
    print(f"[RUN] Spark Analytics: EMR cluster {cluster_id}")
    print(f"[SIGNALS] Buy Signals: {signal_count} detected")
    print(f"[COST] Estimated Cost: $0.15 - $0.25")
    print("=" * 70)


# ============================================================================
# DAG Definition
# ============================================================================

with DAG(
    dag_id="market_data_pipeline_emr",
    default_args=default_args,
    description="Market data collection + Spark analytics on EMR (transient cluster)",
    schedule="0 18 * * *",  # 6 PM ET daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["market-data", "emr", "spark", "production"],
) as dag:

    # Phase 1: Data Collection
    collect = PythonOperator(
        task_id="collect_market_data",
        python_callable=collect_market_data,
    )

    # Phase 2: EMR Cluster + Spark Steps
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name=AWS_REGION,
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        region_name=AWS_REGION,
    )

    # Wait for the last step (VolatilityMetrics) to complete
    wait_for_steps = EmrStepSensor(
        task_id="wait_for_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[-1] }}",
        region_name=AWS_REGION,
        poke_interval=30,
        timeout=1800,  # 30 minutes
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        region_name=AWS_REGION,
        trigger_rule="all_done",  # Terminate even if steps failed
    )

    # Phase 3: Buy Signal Alerts (after Spark analytics)
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
    collect >> create_cluster >> add_steps >> wait_for_steps
    wait_for_steps >> [terminate_cluster, alerts]
    [terminate_cluster, alerts] >> summary
