"""
AWS Glue Tasks for Market Data Pipeline
Simpler alternative to EMR - serverless Spark with zero cluster management
"""

from prefect import task, flow
from prefect.artifacts import create_markdown_artifact, create_link_artifact
from prefect.events import emit_event
import boto3
import time
import os
from typing import Dict, Optional
from datetime import datetime

# Configuration
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Initialize AWS clients
glue = boto3.client('glue', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)


@task(
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
    tags=["glue", "setup"]
)
def create_glue_jobs_if_not_exist() -> Dict[str, str]:
    """
    Create Glue jobs for each Spark script (one-time setup)
    
    Returns:
        dict: Job names created
    """
    
    print("[SETUP] Setting up Glue jobs...")
    
    # Define jobs to create
    jobs = [
        {
            'name': 'market-data-daily-analytics',
            'script': 's3://{}/spark_jobs/daily_analytics.py'.format(S3_BUCKET),
            'description': 'Daily price statistics aggregation'
        },
        {
            'name': 'market-data-ml-features',
            'script': 's3://{}/spark_jobs/ml_features.py'.format(S3_BUCKET),
            'description': 'ML feature engineering with window functions'
        },
        {
            'name': 'market-data-volatility',
            'script': 's3://{}/spark_jobs/volatility_metrics.py'.format(S3_BUCKET),
            'description': 'Volatility metrics calculation'
        }
    ]
    
    created_jobs = {}
    
    for job_def in jobs:
        job_name = job_def['name']
        
        try:
            # Check if job exists
            glue.get_job(JobName=job_name)
            print(f"  [OK] Job exists: {job_name}")
            created_jobs[job_name] = 'exists'
            
        except glue.exceptions.EntityNotFoundException:
            # Create the job
            print(f"  Creating job: {job_name}...")
            
            glue.create_job(
                Name=job_name,
                Description=job_def['description'],
                Role='AWSGlueServiceRole-MarketData',  # Must exist (see setup guide)
                Command={
                    'Name': 'glueetl',  # Standard Glue ETL job
                    'ScriptLocation': job_def['script'],
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--job-language': 'python',
                    '--enable-metrics': 'true',
                    '--enable-spark-ui': 'true',
                    '--spark-event-logs-path': f's3://{S3_BUCKET}/glue-logs/spark-ui/',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--enable-glue-datacatalog': 'true',
                    '--S3_BUCKET': S3_BUCKET,  # Pass to script
                    '--conf': 'spark.sql.legacy.parquet.nanosAsLong=true',
                },
                MaxRetries=2,
                Timeout=30,  # 30 minutes max
                GlueVersion='4.0',  # Latest Glue version (Spark 3.3)
                NumberOfWorkers=3,  # 1 driver + 2 workers
                WorkerType='G.1X',  # 4 vCPU, 16 GB memory per worker
            )
            
            print(f"  [OK] Created: {job_name}")
            created_jobs[job_name] = 'created'
    
    print(f"\n[OK] All Glue jobs ready ({len(created_jobs)} jobs)")
    
    return created_jobs


@task(
    log_prints=True,
    tags=["glue", "execute"]
)
def run_glue_job(job_name: str) -> str:
    """
    Trigger a Glue job (async - doesn't wait)
    
    Args:
        job_name: Name of Glue job to run
        
    Returns:
        str: Job run ID
    """
    
    print(f"[RUN]  Starting Glue job: {job_name}")
    
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            '--S3_BUCKET': S3_BUCKET,
            '--execution_date': datetime.now().isoformat()
        }
    )
    
    job_run_id = response['JobRunId']
    
    print(f"  [OK] Job started: {job_run_id}")
    
    # Create artifact with link to Glue console
    console_url = (
        f"https://{AWS_REGION}.console.aws.amazon.com/gluestudio/home"
        f"?region={AWS_REGION}#/job/{job_name}/run/{job_run_id}"
    )
    
    create_link_artifact(
        key=f"glue-job-{job_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        link=console_url,
        description=f"Glue job: {job_name}"
    )
    
    # Emit event
    emit_event(
        event="glue.job.started",
        resource={
            "prefect.resource.id": f"glue.job.{job_name}",
            "prefect.resource.name": job_name
        },
        payload={
            "job_name": job_name,
            "job_run_id": job_run_id,
            "console_url": console_url
        }
    )
    
    return job_run_id


@task(
    log_prints=True,
    tags=["glue", "monitor"]
)
def wait_for_glue_job(job_name: str, job_run_id: str, timeout_minutes: int = 30) -> Dict:
    """
    Wait for Glue job to complete
    
    Args:
        job_name: Name of Glue job
        job_run_id: Job run ID
        timeout_minutes: Max time to wait
        
    Returns:
        dict: Job run details
    """
    
    print(f"[WAIT] Waiting for job: {job_name}")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while True:
        # Check if timeout
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Job {job_name} exceeded {timeout_minutes} minute timeout")
        
        # Get job status
        response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        job_run = response['JobRun']
        status = job_run['JobRunState']
        
        if status == 'SUCCEEDED':
            duration = (time.time() - start_time) / 60
            print(f"  [OK] Job completed in {duration:.1f} minutes")
            
            # Emit success event
            emit_event(
                event="glue.job.succeeded",
                resource={"prefect.resource.id": f"glue.job.{job_name}"},
                payload={
                    "job_name": job_name,
                    "job_run_id": job_run_id,
                    "duration_minutes": duration,
                    "execution_time": job_run.get('ExecutionTime', 0)
                }
            )
            
            return {
                'status': 'SUCCEEDED',
                'duration_minutes': duration,
                'execution_time': job_run.get('ExecutionTime', 0),
                'job_run_id': job_run_id
            }
            
        elif status in ['FAILED', 'STOPPED', 'ERROR', 'TIMEOUT']:
            error_msg = job_run.get('ErrorMessage', 'Unknown error')
            print(f"  [ERROR] Job failed: {error_msg}")
            
            # Emit failure event
            emit_event(
                event="glue.job.failed",
                resource={"prefect.resource.id": f"glue.job.{job_name}"},
                payload={
                    "job_name": job_name,
                    "job_run_id": job_run_id,
                    "error": error_msg
                }
            )
            
            raise RuntimeError(f"Glue job {job_name} failed: {error_msg}")
        
        elif status == 'RUNNING':
            elapsed = (time.time() - start_time) / 60
            print(f"  [WAIT] Running... ({elapsed:.1f} min elapsed)")
            time.sleep(30)  # Check every 30 seconds
        
        else:
            # STARTING, STOPPING, etc.
            print(f"  [WAIT] Status: {status}")
            time.sleep(10)


@flow(
    name="market-data-glue-analytics",
    description="Run Spark analytics using AWS Glue (serverless)",
    log_prints=True
)
def glue_analytics_flow():
    """
    Run all Spark analytics jobs using AWS Glue
    
    Simpler than EMR:
    - No cluster management
    - No VPC configuration
    - Pay only for job runtime
    - Automatic scaling
    """
    
    start_time = datetime.now()
    
    print(f"\n{'='*70}")
    print(f"[START] AWS Glue Analytics Pipeline")
    print(f"{'='*70}")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"{'='*70}\n")
    
    # Step 1: Ensure Glue jobs exist (one-time setup)
    jobs = create_glue_jobs_if_not_exist()
    
    # Step 2: Run all jobs in sequence
    job_results = []
    
    for job_name in [
        'market-data-daily-analytics',
        'market-data-ml-features', 
        'market-data-volatility'
    ]:
        try:
            print(f"\n{'---'*70}")
            print(f"Running: {job_name}")
            print(f"{'---'*70}")
            
            # Start job
            job_run_id = run_glue_job(job_name)
            
            # Wait for completion
            result = wait_for_glue_job(job_name, job_run_id)
            
            job_results.append({
                'job_name': job_name,
                'status': 'success',
                'duration_minutes': result['duration_minutes'],
                'job_run_id': job_run_id
            })
            
            print(f"[OK] {job_name} completed")
            
        except Exception as e:
            print(f"[ERROR] {job_name} failed: {e}")
            
            job_results.append({
                'job_name': job_name,
                'status': 'failed',
                'error': str(e)
            })
    
    # Step 3: Create summary
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds() / 60
    
    success_count = len([r for r in job_results if r['status'] == 'success'])
    fail_count = len(job_results) - success_count
    
    # Calculate total Glue execution time (actual compute time billed)
    total_execution_time = sum(
        r.get('duration_minutes', 0) 
        for r in job_results 
        if r['status'] == 'success'
    )
    
    # Estimate cost (Glue pricing: $0.44/DPU-hour, 3 workers = ~3 DPUs)
    estimated_cost = (total_execution_time / 60) * 0.44 * 3
    
    summary_md = f"""# [TARGET] Glue Analytics Pipeline Summary

## Execution Overview
- **Started:** {start_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Completed:** {end_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Total Duration:** {total_duration:.1f} minutes
- **Actual Compute Time:** {total_execution_time:.1f} minutes

## Results
- **Total Jobs:** {len(job_results)}
- **[OK] Successful:** {success_count}
- **[ERROR] Failed:** {fail_count}
- **Success Rate:** {(success_count/len(job_results)*100):.1f}%

## Job Details
"""
    
    for result in job_results:
        status_icon = "[OK]" if result['status'] == 'success' else "[ERROR]"
        job_name = result['job_name']
        
        if result['status'] == 'success':
            duration = result['duration_minutes']
            summary_md += f"- {status_icon} **{job_name}** - {duration:.1f} min\n"
        else:
            error = result['error']
            summary_md += f"- {status_icon} **{job_name}** - Failed: {error}\n"
    
    summary_md += f"""
## Cost Estimate
- **Compute Time:** {total_execution_time:.1f} minutes
- **Estimated Cost:** ${estimated_cost:.2f}
- **Workers:** 3 (G.1X: 4 vCPU, 16 GB each)

## Output Locations
- Daily Stats: `s3://{S3_BUCKET}/analytics/daily_stats/`
- ML Features: `s3://{S3_BUCKET}/analytics/ml_features/`
- Volatility: `s3://{S3_BUCKET}/analytics/volatility_metrics/`

## Next Steps
Query with Athena:
```sql
SELECT * FROM analytics.daily_stats ORDER BY ingestion_date DESC LIMIT 10;
```

View in Glue Console:
https://{AWS_REGION}.console.aws.amazon.com/gluestudio/home?region={AWS_REGION}#/jobs
"""
    
    create_markdown_artifact(
        key=f"glue-summary-{start_time.strftime('%Y%m%d%H%M%S')}",
        markdown=summary_md,
        description="Glue analytics pipeline summary"
    )
    
    print(f"\n{'='*70}")
    print(f"[COMPLETE] Pipeline Complete!")
    print(f"{'='*70}")
    print(f"Duration: {total_duration:.1f} minutes")
    print(f"Success: {success_count}/{len(job_results)}")
    print(f"Estimated Cost: ${estimated_cost:.2f}")
    print(f"{'='*70}\n")
    
    # Emit completion event
    emit_event(
        event="glue.pipeline.completed",
        resource={"prefect.resource.id": "market-data-glue-pipeline"},
        payload={
            "total_duration_minutes": total_duration,
            "compute_time_minutes": total_execution_time,
            "jobs_succeeded": success_count,
            "jobs_failed": fail_count,
            "estimated_cost": estimated_cost
        }
    )
    
    return {
        'job_results': job_results,
        'total_duration_minutes': total_duration,
        'estimated_cost': estimated_cost
    }


if __name__ == "__main__":
    glue_analytics_flow()
