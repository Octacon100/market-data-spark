"""
Market Data Pipeline with Spark Analytics on AWS Glue (Serverless)
Alternative to EMR - simpler, no VPC configuration needed
"""

from prefect import flow
from market_data_flow import market_data_pipeline
from glue_tasks import glue_analytics_flow
from buy_signal_alerts import buy_signal_alert_flow
from datetime import datetime
import os

@flow(
    name="market-data-pipeline-with-glue",
    description="Complete pipeline: Data collection + Spark analytics on AWS Glue (serverless)",
    log_prints=True
)
def market_data_pipeline_with_glue():
    """
    End-to-end pipeline using AWS Glue instead of EMR:
    1. Collect market data from API
    2. Validate and store to S3
    3. Trigger Spark analytics on AWS Glue (serverless)
    4. Glue jobs auto-scale and terminate
    
    Benefits vs EMR:
    - No VPC configuration needed
    - Faster startup (~1 min vs ~6 min)
    - Simpler infrastructure
    - 30% lower cost
    
    Returns:
        dict: Pipeline results including Glue job info
    """
    
    print("\n" + "="*70)
    print("[START] Market Data Pipeline with Spark Analytics (Glue)")
    print("="*70)
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   S3 Bucket: {os.getenv('S3_BUCKET')}")
    print(f"   Compute: AWS Glue (Serverless)")
    print("="*70 + "\n")
    
    # Phase 1: Data Collection
    print("[DATA] PHASE 1: Market Data Collection")
    print("-"*70)
    
    data_results = market_data_pipeline()
    
    # Check if we collected any data
    success_count = len(data_results["successful"])
    
    if success_count == 0:
        print("\n[WARNING] No data collected - skipping Spark analytics")
        return {
            'phase': 'data_collection_only',
            'data_results': data_results,
            'glue_results': None,
            'message': 'No data collected, Glue not triggered'
        }
    
    print(f"\n[OK] Data collection complete: {success_count} symbols processed")
    
    # Phase 2: Spark Analytics on Glue
    print("\n" + "="*70)
    print("[RUN] PHASE 2: Spark Analytics on AWS Glue")
    print("-"*70)
    
    glue_results = glue_analytics_flow()
    
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
    print(f"[RUN] Spark Analytics: {len(glue_results.get('job_results', []))} Glue jobs")

    # Calculate success
    glue_success = len([j for j in glue_results.get('job_results', []) if j.get('status') == 'success'])
    print(f"[OK] Glue Jobs Succeeded: {glue_success}/{len(glue_results.get('job_results', []))}")
    print(f"[SIGNALS] Buy Signals: {signal_count} detected")

    # Show cost
    estimated_cost = glue_results.get('estimated_cost', 0)
    print(f"[COST] Estimated Cost: ${estimated_cost:.2f}")
    print("="*70 + "\n")

    return {
        'phase': 'complete',
        'data_results': data_results,
        'glue_results': glue_results,
        'signal_results': signal_results,
        'summary': {
            'symbols_processed': success_count,
            'glue_jobs_run': len(glue_results.get('job_results', [])),
            'glue_jobs_succeeded': glue_success,
            'buy_signals_detected': signal_count,
            'estimated_cost': estimated_cost,
            'message': 'Pipeline complete - Glue jobs finished'
        }
    }


if __name__ == "__main__":
    # Run the complete pipeline with Glue
    # results = market_data_pipeline_with_glue()
    # glue_analytics_flow()
    market_data_pipeline_with_glue()
    print("\n[COMPLETE] Pipeline execution complete!")
    print(f"   View results in Prefect Cloud: https://app.prefect.cloud")
