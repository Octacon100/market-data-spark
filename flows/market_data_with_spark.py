"""
Market Data Pipeline with Spark Analytics on EMR
Complete integration: Data collection → Spark analytics → Results
"""

from prefect import flow
from market_data_flow import market_data_pipeline
from emr_tasks import spark_analytics_flow
from buy_signal_alerts import buy_signal_alert_flow
from datetime import datetime
import os

@flow(
    name="market-data-pipeline-with-spark",
    description="Complete pipeline: Data collection + Spark analytics on EMR",
    log_prints=True
)
def market_data_pipeline_with_spark():
    """
    End-to-end pipeline:
    1. Collect market data from API
    2. Validate and store to S3
    3. Trigger Spark analytics on EMR
    4. EMR cluster auto-terminates
    
    Returns:
        dict: Pipeline results including EMR cluster info
    """
    
    print("\n" + "="*70)
    print("[START] Market Data Pipeline with Spark Analytics")
    print("="*70)
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   S3 Bucket: {os.getenv('S3_BUCKET')}")
    print("="*70 + "\n")
    
    # Phase 1: Data Collection
    print("[DATA] PHASE 1: Market Data Collection")
    print("-"*70)
    
    data_results = market_data_pipeline()
    
    # Check if we collected any data
    success_count = len(data_results["successful"])
    
    if success_count == 0:
        print("\n[WARNING]️  No data collected - skipping Spark analytics")
        return {
            'phase': 'data_collection_only',
            'data_results': data_results,
            'emr_results': None,
            'message': 'No data collected, EMR not triggered'
        }
    
    print(f"\n✅ Data collection complete: {success_count} symbols processed")
    
    # Phase 2: Spark Analytics
    print("\n" + "="*70)
    print("⚡ PHASE 2: Spark Analytics on EMR")
    print("-"*70)
    
    emr_results = spark_analytics_flow()
    
    # Phase 3: Buy Signal Alerts
    print("\n" + "="*70)
    print("[SIGNALS] PHASE 3: Buy Signal Detection & Alerts")
    print("-"*70)

    signal_results = buy_signal_alert_flow(bucket=os.getenv('S3_BUCKET'))
    signal_count = signal_results.get('count', 0)

    # Phase 4: Summary
    print("\n" + "="*70)
    print("[COMPLETE] PIPELINE SUMMARY")
    print("="*70)
    print(f"[OK] Data Collection: {success_count} symbols")
    print(f"[RUN] Spark Analytics: Cluster {emr_results.get('cluster_id', 'N/A')}")
    print(f"[SIGNALS] Buy Signals: {signal_count} detected")
    print(f"[WAIT] Status: Jobs running on EMR (auto-terminates when done)")
    print(f"[COST] Estimated Cost: $0.15 - $0.25")
    print("="*70 + "\n")

    return {
        'phase': 'complete',
        'data_results': data_results,
        'emr_results': emr_results,
        'signal_results': signal_results,
        'summary': {
            'symbols_processed': success_count,
            'emr_cluster_id': emr_results.get('cluster_id'),
            'buy_signals_detected': signal_count,
            'start_time': emr_results.get('start_time'),
            'message': 'Pipeline complete - EMR cluster running'
        }
    }


if __name__ == "__main__":
    # Run the complete pipeline
    results = market_data_pipeline_with_spark()
    
    print("\n🎉 Pipeline execution complete!")
    print(f"   View results in Prefect Cloud: https://app.prefect.cloud")
