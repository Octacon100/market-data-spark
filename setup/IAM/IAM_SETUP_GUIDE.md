# IAM Policy Setup Guide for Market Data Pipeline

## Two Options

### Option 1: Granular Policy (Recommended for Production)
**File:** `MarketDataPipelinePolicy.json`

✅ **Pros:**
- Least privilege access
- More secure
- Meets compliance requirements
- Specific to your use case

❌ **Cons:**
- Longer policy
- May need adjustments if you add features

**Use this if:** You're in a corporate environment or want production-grade security.

---

### Option 2: Minimal Policy (Recommended for Portfolio/Learning)
**File:** `MarketDataPipelinePolicy-Minimal.json`

✅ **Pros:**
- Simpler to set up
- Less likely to hit permission errors
- Faster iteration

❌ **Cons:**
- More permissive than needed
- Not ideal for production

**Use this if:** You're building a portfolio project or learning.

---

## Setup Instructions

### Step 1: Create the IAM Policy

**Using AWS Console:**

1. Open AWS Console → IAM → Policies
2. Click **"Create policy"**
3. Click **"JSON"** tab
4. Paste contents of `MarketDataPipelinePolicy.json` (or `-Minimal.json`)
5. Click **"Next: Tags"** (optional)
6. Click **"Next: Review"**
7. Name: `MarketDataPipelinePolicy`
8. Description: `Permissions for market data pipeline with Prefect and EMR`
9. Click **"Create policy"**

**Using AWS CLI:**

```bash
# Option 1: Granular policy
aws iam create-policy \
    --policy-name MarketDataPipelinePolicy \
    --policy-document file://MarketDataPipelinePolicy.json \
    --description "Permissions for market data pipeline with Prefect and EMR"

# Option 2: Minimal policy
aws iam create-policy --policy-name MarketDataPipelinePolicy --policy-document file://MarketDataPipelinePolicy-Minimal.json   --description "Minimal permissions for market data pipeline"
```

---

### Step 2: Attach Policy to Your IAM User

**Using AWS Console:**

1. IAM → Users → [Your Username]
2. Click **"Add permissions"**
3. Click **"Attach existing policies directly"**
4. Search for: `MarketDataPipelinePolicy`
5. Check the box
6. Click **"Next: Review"**
7. Click **"Add permissions"**

**Using AWS CLI:**

```bash
# Get your policy ARN
POLICY_ARN=$(aws iam list-policies \
    --query 'Policies[?PolicyName==`MarketDataPipelinePolicy`].Arn' \
    --output text)

# Attach to your user
aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn $POLICY_ARN

# Verify
aws iam list-attached-user-policies --user-name YOUR_USERNAME
```

---

## What Each Permission Does

### **S3 Permissions**
```json
"s3:CreateBucket"        → Create your market-data-* bucket
"s3:ListBucket"          → List files in bucket
"s3:GetObject"           → Download files (Spark reads data)
"s3:PutObject"           → Upload files (pipeline writes data)
"s3:DeleteObject"        → Clean up old files
```

### **EMR Permissions**
```json
"elasticmapreduce:RunJobFlow"       → Create EMR cluster
"elasticmapreduce:DescribeCluster"  → Check cluster status
"elasticmapreduce:TerminateJobFlows" → Shut down cluster
"elasticmapreduce:AddJobFlowSteps"  → Submit Spark jobs
"elasticmapreduce:ListSteps"        → Monitor job progress
```

### **EC2 Permissions**
```json
"ec2:CreateKeyPair"        → Create SSH key pair
"ec2:DescribeInstances"    → EMR needs to see EC2 instances
"ec2:DescribeSubnets"      → EMR needs network info
"ec2:DescribeSecurityGroups" → EMR security setup
```

### **IAM Permissions**
```json
"iam:PassRole"    → EMR needs to assume service roles
"iam:GetRole"     → Verify roles exist
```

### **Athena/Glue Permissions**
```json
"athena:StartQueryExecution" → Run SQL queries
"glue:CreateTable"           → Create Athena tables
"glue:GetTable"              → Read table metadata
```

---

## Troubleshooting Permission Issues

### Error: "User is not authorized to perform: elasticmapreduce:RunJobFlow"

**Solution:** Attach EMR permissions
```bash
# Quick fix: Add AWS managed policy (development only)
aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2
```

### Error: "Access Denied" when creating S3 bucket

**Solution:** Check S3 permissions and bucket naming
```bash
# Verify S3 permissions
aws iam get-user-policy \
    --user-name YOUR_USERNAME \
    --policy-name MarketDataPipelinePolicy

# Bucket name must start with "market-data-"
# Update .env file: S3_BUCKET=market-data-yourname-123456
```

### Error: "Cannot pass role EMR_DefaultRole"

**Solution:** Add PassRole permission
```bash
# Add IAM PassRole inline policy
aws iam put-user-policy \
    --user-name YOUR_USERNAME \
    --policy-name EMRPassRole \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                "arn:aws:iam::*:role/EMR_DefaultRole",
                "arn:aws:iam::*:role/EMR_EC2_DefaultRole"
            ]
        }]
    }'
```

### Error: "Could not find EMR default roles"

**Solution:** Create EMR default roles
```bash
# Run this command
aws emr create-default-roles

# Verify
aws iam list-roles | grep EMR_
```

---

## Testing Your Permissions

Run this script to test all required permissions:

```bash
# test-permissions.sh

echo "Testing AWS Permissions for Market Data Pipeline"
echo "================================================"

# Test 1: S3
echo -n "Testing S3 bucket creation... "
aws s3 mb s3://market-data-test-$RANDOM 2>/dev/null && echo "✓" || echo "✗"

# Test 2: EMR roles
echo -n "Testing EMR default roles... "
aws iam get-role --role-name EMR_DefaultRole >/dev/null 2>&1 && echo "✓" || echo "✗"

# Test 3: EC2 key pairs
echo -n "Testing EC2 key pair creation... "
aws ec2 describe-key-pairs >/dev/null 2>&1 && echo "✓" || echo "✗"

# Test 4: Athena
echo -n "Testing Athena access... "
aws athena list-work-groups >/dev/null 2>&1 && echo "✓" || echo "✗"

# Test 5: Glue
echo -n "Testing Glue catalog access... "
aws glue get-databases >/dev/null 2>&1 && echo "✓" || echo "✗"

echo ""
echo "If all tests show ✓, you're good to go!"
```

---

## Minimum Permissions Quick Reference

If you just want to get started quickly:

```bash
# Attach AWS managed policies (development only!)
aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2

aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess

# For production, use the custom policies instead!
```

---

## Security Best Practices

### For Portfolio Projects:
✅ Use the minimal policy  
✅ Restrict S3 to buckets starting with `market-data-`  
✅ Delete resources after demo  
✅ Don't commit AWS credentials to Git  

### For Production:
✅ Use the granular policy  
✅ Enable MFA on IAM user  
✅ Use separate IAM users for dev/prod  
✅ Enable CloudTrail logging  
✅ Rotate access keys every 90 days  
✅ Use IAM roles instead of long-lived credentials  

---

## Next Steps

After setting up IAM permissions:

1. ✓ Create IAM policy
2. ✓ Attach to your user
3. Run: `.\setup\powershell\setup-emr.ps1`
4. Test: `aws s3 ls` (should work)
5. Test: `aws emr list-clusters` (should work)

If you get permission errors, check the Troubleshooting section above!

---

## Policy Files Included

- `MarketDataPipelinePolicy.json` - Granular (production-ready)
- `MarketDataPipelinePolicy-Minimal.json` - Minimal (quick start)

Choose based on your needs!
