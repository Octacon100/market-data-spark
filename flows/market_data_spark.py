"""
Market Data Pipeline with Spark Analytics - Choose Your Backend
Run with either EMR (infrastructure control) or Glue (serverless)
"""

from prefect import flow
from market_data_flow import market_data_pipeline
from emr_tasks import spark_analytics_flow as emr_spark_flow
from glue_tasks import glue_analytics_flow
from buy_signal_alerts import buy_signal_alert_flow
from datetime import datetime
import os

@flow(
    name="market-data-pipeline-spark",
    description="Complete pipeline with choice of EMR or Glue for Spark analytics",
    log_prints=True
)
def market_data_pipeline_spark(backend: str = "glue"):
    """
    End-to-end pipeline with choice of Spark backend:
    1. Collect market data from API
    2. Validate and store to S3
    3. Trigger Spark analytics on chosen backend
    
    Args:
        backend: Either "emr" or "glue" (default: "glue")
            - "emr": Traditional EMR cluster (more control, VPC required)
            - "glue": Serverless Glue (simpler, faster, cheaper)
    
    Returns:
        dict: Pipeline results including backend-specific info
    """
    
    # Validate backend choice
    backend = backend.lower()
    if backend not in ["emr", "glue"]:
        raise ValueError(f"Invalid backend '{backend}'. Choose 'emr' or 'glue'")
    
    print("\n" + "="*70)
    print(f"[START] Market Data Pipeline with Spark ({backend.upper()})")
    print("="*70)
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   S3 Bucket: {os.getenv('S3_BUCKET')}")
    print(f"   Compute Backend: {backend.upper()}")
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
            'spark_results': None,
            'backend': backend,
            'message': f'No data collected, {backend.upper()} not triggered'
        }
    
    print(f"\n[OK] Data collection complete: {success_count} symbols processed")
    
    # Phase 2: Spark Analytics (backend-specific)
    print("\n" + "="*70)
    print(f"[RUN] PHASE 2: Spark Analytics on {backend.upper()}")
    print("-"*70)
    
    if backend == "emr":
        # Use EMR
        print("[SETUP] Creating EMR cluster...")
        spark_results = emr_spark_flow()
        
        cluster_id = spark_results.get('cluster_id', 'N/A')
        cost_estimate = "$0.15 - $0.25"
        
        print(f"\n[OK] EMR cluster created: {cluster_id}")
        print(f"[RUN] Spark jobs submitted and running")
        print(f"[SETUP] Cluster will auto-terminate when complete")
        
    else:  # glue
        # Use Glue
        print("[SETUP] Triggering Glue jobs (serverless)...")
        spark_results = glue_analytics_flow()
        
        job_count = len(spark_results.get('job_results', []))
        job_success = len([j for j in spark_results.get('job_results', []) 
                          if j.get('status') == 'success'])
        cost_estimate = f"${spark_results.get('estimated_cost', 0):.2f}"
        
        print(f"\n[OK] Glue jobs completed: {job_success}/{job_count} successful")
    
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
    print(f"[RUN] Spark Backend: {backend.upper()}")

    if backend == "emr":
        print(f"[SETUP] EMR Cluster: {spark_results.get('cluster_id', 'N/A')}")
        print(f"[WAIT] Status: Jobs running (cluster auto-terminates)")
    else:
        print(f"[OK] Glue Jobs: {job_success}/{job_count} succeeded")

    print(f"[SIGNALS] Buy Signals: {signal_count} detected")
    print(f"[COST] Estimated Cost: {cost_estimate}")
    print("="*70 + "\n")

    return {
        'phase': 'complete',
        'backend': backend,
        'data_results': data_results,
        'spark_results': spark_results,
        'signal_results': signal_results,
        'summary': {
            'symbols_processed': success_count,
            'backend_used': backend,
            'buy_signals_detected': signal_count,
            'estimated_cost': cost_estimate,
            'message': f'Pipeline complete - {backend.upper()} analytics finished'
        }
    }


if __name__ == "__main__":
    import sys
    
    # Check command line argument for backend choice
    backend = "glue"  # Default
    
    if len(sys.argv) > 1:
        backend = sys.argv[1].lower()
        if backend not in ["emr", "glue"]:
            print(f"[ERROR] Invalid backend: {backend}")
            print("Usage: python market_data_spark.py [emr|glue]")
            print("Example: python market_data_spark.py glue")
            sys.exit(1)
    
    print(f"\n[SETUP] Running with backend: {backend.upper()}\n")
    
    # Run the pipeline
    results = market_data_pipeline_spark(backend=backend)
    
    print("\n[COMPLETE] Pipeline execution complete!")
    print(f"   Backend used: {backend.upper()}")
    print(f"   View results in Prefect Cloud: https://app.prefect.cloud")
