# Fabric Spark Notebooks

These scripts are the Spark analytics jobs for the Microsoft Fabric edition of the pipeline.
Upload each one to your Fabric workspace as a notebook.

## Setup

### 1. Create a Lakehouse

In the Fabric portal, create a Lakehouse named `MarketDataLakehouse` (or set `FABRIC_LAKEHOUSE_NAME`
to match whatever name you choose).

### 2. Upload notebooks

For each `.py` file in this directory:
1. Open your Fabric workspace
2. Click **+ New item** > **Notebook**
3. Paste the file contents into the notebook code cell
4. Save the notebook and note its **item ID** from the URL:
   `https://app.fabric.microsoft.com/groups/<workspace-id>/synapsenotebooks/<notebook-id>`

### 3. Configure environment variables

Add these to your `.env` file (or Prefect Variables):

```
FABRIC_WORKSPACE_ID=<your-workspace-guid>
FABRIC_LAKEHOUSE_NAME=MarketDataLakehouse
FABRIC_NOTEBOOK_DAILY_ANALYTICS=<daily_analytics notebook GUID>
FABRIC_NOTEBOOK_ML_FEATURES=<ml_features notebook GUID>
FABRIC_NOTEBOOK_VOLATILITY=<volatility_metrics notebook GUID>
```

### 4. Deploy the Fabric flows

```bash
docker-compose exec worker python flows/deploy_fabric.py
```

## Notebooks

| File | Purpose | Reads from | Writes to |
|------|---------|------------|-----------|
| `daily_analytics.py` | Daily price statistics | `Files/processed/stocks/` | `Files/analytics/daily_stats/` |
| `ml_features.py` | Moving averages, momentum, window features | `Files/processed/stocks/` | `Files/analytics/ml_features/` |
| `volatility_metrics.py` | Intraday range, ATR, annualised vol | `Files/processed/stocks/` | `Files/analytics/volatility_metrics/` |

## Differences from the AWS Glue version

| Aspect | AWS Glue | Microsoft Fabric |
|--------|----------|-----------------|
| Storage | S3 | OneLake (ADLS Gen2) |
| Compute | Glue jobs (G.1X workers) | Fabric Spark notebooks |
| Auth | IAM role | Azure service principal |
| Path prefix | `s3://bucket/...` | `abfss://<workspace>@onelake.../` |
| Trigger | `boto3 glue.start_job_run()` | Fabric REST API |
| Cost model | Per DPU-hour | Fabric CU capacity |
