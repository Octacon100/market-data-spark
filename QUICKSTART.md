# Quick Start Guide

Get your market data pipeline with Spark analytics running in 30 minutes!

## Prerequisites Checklist

- [ ] AWS account with free tier
- [ ] Alpha Vantage API key (free from https://www.alphavantage.co)
- [ ] Prefect Cloud account (free from https://app.prefect.cloud)
- [ ] Python 3.11+ installed
- [ ] AWS CLI installed and configured

## Step-by-Step Setup

### 1. Environment Setup (5 minutes)

```bash
# Extract the package
cd market-data-spark-emr

# Install Python dependencies
pip install -r requirements.txt

# Create environment file
cp .env.example .env

# Edit .env with your credentials
# Required: ALPHA_VANTAGE_API_KEY, S3_BUCKET
nano .env
```

### 2. AWS Configuration (10 minutes)

```bash
# Configure AWS CLI
aws configure
# Enter your Access Key, Secret Key, Region (us-east-1)

# Create S3 bucket (use unique name)
export BUCKET_NAME="market-data-$(date +%s)"
aws s3 mb s3://$BUCKET_NAME

# Add to .env file
echo "S3_BUCKET=$BUCKET_NAME" >> .env

# Create EMR default roles
aws emr create-default-roles

# Create EC2 key pair (for EMR access)
aws ec2 create-key-pair --key-name market-data-key \
  --query 'KeyMaterial' --output text > market-data-key.pem
chmod 400 market-data-key.pem

# Add key pair name to .env
echo "EMR_KEY_PAIR=market-data-key" >> .env
```

### 3. Prefect Setup (5 minutes)

```bash
# Login to Prefect Cloud
prefect cloud login
# Paste your API key when prompted

# Create work pool
prefect work-pool create local-laptop --type process

# Verify connection
prefect work-pool ls
```

### 4. Upload Spark Scripts (2 minutes)

```bash
# Load environment variables
source .env  # or: export $(cat .env | xargs)

# Upload Spark jobs to S3
python setup/upload_spark_scripts.py
```

### 5. Test the Pipeline (5 minutes)

```bash
# Terminal 1: Start Prefect worker
prefect worker start --pool local-laptop

# Terminal 2: Run the pipeline
python flows/market_data_with_spark.py
```

Expected output:
```
🚀 Market Data Pipeline with Spark Analytics
===================================================================
📊 PHASE 1: Market Data Collection
...
✅ Data collection complete: 5 symbols processed
⚡ PHASE 2: Spark Analytics on EMR
...
✅ EMR cluster created: j-XXXXXXXXXXXXX
```

### 6. Monitor Progress

**Prefect Cloud UI:**
```
https://app.prefect.cloud
```
- View flow runs
- Check task execution
- See artifacts and logs

**AWS EMR Console:**
```
https://console.aws.amazon.com/emr
```
- Find your cluster: `MarketData-Transient-YYYYMMDD-HHMMSS`
- Monitor step progress
- View logs

**Expected Timeline:**
- 0-15 min: Data collection
- 15-20 min: EMR cluster starting
- 20-30 min: Spark jobs running
- 30-35 min: EMR cluster terminating
- **Total: ~35 minutes, Cost: ~$0.20**

### 7. Query Results (5 minutes)

```bash
# List results in S3
aws s3 ls s3://$S3_BUCKET/analytics/ --recursive

# You should see:
# analytics/daily_stats/
# analytics/ml_features/
# analytics/volatility_metrics/
```

Create Athena tables (in AWS Console):
```sql
-- See sql/create_athena_tables.sql
```

Query your data:
```sql
SELECT * FROM analytics.daily_stats
WHERE date = CURRENT_DATE
ORDER BY symbol;
```

## Optional: Set Up Daily Schedule

```bash
# Deploy with schedule
python flows/deploy.py

# Keep worker running
prefect worker start --pool local-laptop
```

Pipeline will now run automatically at 6 PM ET daily!

## Troubleshooting

### "S3_BUCKET environment variable not set"
```bash
# Make sure .env is loaded
source .env
# Or use direnv for automatic loading
```

### "EMR_DefaultRole does not exist"
```bash
# Create default IAM roles
aws emr create-default-roles
```

### "Rate limit exceeded" (Alpha Vantage)
```bash
# In .env, reduce symbols
SYMBOLS=AAPL,GOOGL  # Instead of 5 symbols
```

### EMR cluster stuck in STARTING
```bash
# Check cluster logs
aws emr describe-cluster --cluster-id j-XXXXXXXXXXXXX

# Common issues:
# 1. Wrong subnet/security group
# 2. No EC2 capacity
# 3. IAM role issues
```

### Spark job fails
```bash
# Download step logs
aws s3 cp s3://$S3_BUCKET/emr-logs/ ./logs/ --recursive

# Check stderr files for error messages
```

## Next Steps

1. **Customize Spark Jobs**: Edit files in `spark_jobs/`
2. **Add More Symbols**: Update SYMBOLS in `.env`
3. **Schedule Changes**: Edit `flows/deploy.py`
4. **Cost Optimization**: See `docs/COST_OPTIMIZATION.md`

## Getting Help

- **Documentation**: See `docs/` folder
- **Troubleshooting**: `docs/TROUBLESHOOTING.md`
- **EMR Patterns**: `docs/EMR_PATTERNS.md`

## Success Checklist

After running successfully, you should have:

- [x] Data in S3 (`s3://bucket/processed/stocks/`)
- [x] Analytics results (`s3://bucket/analytics/`)
- [x] EMR cluster auto-terminated
- [x] Flow run visible in Prefect Cloud
- [x] Total cost < $0.25

Congratulations! Your pipeline is working! 🎉

## What You Built

You now have a production-ready data pipeline that:
- ✅ Collects real-time market data
- ✅ Validates data quality
- ✅ Stores in partitioned S3
- ✅ Runs distributed Spark analytics
- ✅ Auto-manages EMR clusters
- ✅ Costs $0.20/run ($6/month)
- ✅ Observable in Prefect Cloud
- ✅ Queryable via Athena

Perfect for your data engineering portfolio! 🚀
