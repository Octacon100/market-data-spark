"""
Upload Spark Scripts to S3
Run this once to upload all Spark job scripts to S3
"""

import boto3
import os
from pathlib import Path

def upload_spark_scripts():
    """Upload all Spark job scripts to S3"""
    
    # Get configuration
    bucket_name = os.getenv('S3_BUCKET')
    if not bucket_name:
        raise ValueError("S3_BUCKET environment variable not set")
    
    print(f"Uploading Spark scripts to s3://{bucket_name}/spark_jobs/")
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Get project root
    project_root = Path(__file__).parent.parent
    spark_jobs_dir = project_root / 'spark_jobs'
    
    # Upload each Python file
    uploaded_count = 0
    
    for script_file in spark_jobs_dir.glob('*.py'):
        s3_key = f'spark_jobs/{script_file.name}'
        
        print(f"  Uploading {script_file.name}...")
        
        with open(script_file, 'rb') as f:
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=f.read(),
                ContentType='text/x-python'
            )
        
        uploaded_count += 1
        print(f"    ✅ s3://{bucket_name}/{s3_key}")
    
    print(f"\n✅ Uploaded {uploaded_count} Spark scripts successfully!")
    print(f"\nScripts available at:")
    print(f"  s3://{bucket_name}/spark_jobs/daily_analytics.py")
    print(f"  s3://{bucket_name}/spark_jobs/ml_features.py")
    print(f"  s3://{bucket_name}/spark_jobs/volatility_metrics.py")


if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    upload_spark_scripts()
