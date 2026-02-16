"""
Convert existing S3 Parquet files from nanosecond to microsecond timestamps.
This fixes compatibility with Spark 3.3 / AWS Glue 4.0.
"""

import boto3
import io
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def convert_parquet_timestamps():
    bucket_name = os.getenv('S3_BUCKET')
    if not bucket_name:
        raise ValueError("S3_BUCKET environment variable not set")

    s3 = boto3.client('s3')
    prefix = 'processed/stocks/'

    print(f"Scanning s3://{bucket_name}/{prefix} for parquet files...")

    # List all parquet files
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])

    if not files:
        print("No parquet files found.")
        return

    print(f"Found {len(files)} parquet files to convert.\n")

    converted = 0
    errors = 0

    for key in files:
        try:
            # Download
            response = s3.get_object(Bucket=bucket_name, Key=key)
            data = response['Body'].read()

            # Read with pandas (handles nanos fine)
            df = pd.read_parquet(io.BytesIO(data))

            # Convert timestamp columns from nanos to micros
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].dt.floor('us')

            # Re-write with microsecond precision
            buf = io.BytesIO()
            df.to_parquet(
                buf,
                engine='pyarrow',
                compression='snappy',
                index=False,
                coerce_timestamps='us',
                allow_truncated_timestamps=True
            )
            buf.seek(0)

            # Upload back to same key
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=buf.getvalue(),
                ContentType='application/octet-stream'
            )

            converted += 1
            print(f"  [OK] {key}")

        except Exception as e:
            errors += 1
            print(f"  [ERROR] {key}: {e}")

    print(f"\nDone. Converted: {converted}, Errors: {errors}")


if __name__ == "__main__":
    convert_parquet_timestamps()
