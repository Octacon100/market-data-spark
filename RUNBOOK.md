# AWS Glue Pipeline Setup & Windows PowerShell Best Practices

**Project:** Market Data Pipeline with AWS Glue + Prefect 3.0  
**Environment:** Windows PowerShell  
**Last Updated:** February 7, 2026  
**Author:** Nathan Low

---

## Table of Contents

1. [AWS Glue IAM Role Setup](#aws-glue-iam-role-setup)
2. [Windows PowerShell Requirements](#windows-powershell-requirements)
3. [Troubleshooting Common Issues](#troubleshooting-common-issues)
4. [Script Creation Guidelines](#script-creation-guidelines)
5. [Validation Checklist](#validation-checklist)
6. [Running the Glue Pipeline](#running-the-glue-pipeline)
7. [Monitoring & Debugging](#monitoring--debugging)

---

## AWS Glue IAM Role Setup

### Problem Encountered

When running AWS Glue jobs through Prefect, encountered this error:

```
Role arn:aws:iam::124909680453:role/service-role/AWSGlueServiceRole-MarketData 
should be given assume role permissions for Glue Service.
```

### Root Cause

The IAM role either:
1. Did not exist
2. Existed but lacked the proper trust policy allowing `glue.amazonaws.com` to assume it

### Solution

Created `create-glue-role-v2.ps1` which:
- Creates the IAM role `AWSGlueServiceRole-MarketData`
- Adds trust policy for AWS Glue service
- Attaches AWS managed policy `AWSGlueServiceRole`
- Adds inline policy for S3 and CloudWatch access

### Setup Steps

```powershell
# 1. Ensure you have IAM permissions (run as admin or root user)
aws sts get-caller-identity

# 2. Run the setup script
.\create-glue-role-v2.ps1

# 3. Wait 30-60 seconds for IAM propagation
Start-Sleep -Seconds 30

# 4. Verify role exists
aws iam get-role --role-name AWSGlueServiceRole-MarketData

# 5. Run your Prefect flow
python your_flow.py
```

### Role Components

**Role ARN:**
```
arn:aws:iam::124909680453:role/service-role/AWSGlueServiceRole-MarketData
```

**Trust Policy (allows Glue to assume this role):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Attached Policies:**
1. AWS Managed: `AWSGlueServiceRole`
2. Inline: `MarketDataS3Access` (S3 + CloudWatch permissions)

**S3 Permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::market-data-pipeline-*/*",
        "arn:aws:s3:::aws-glue-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::market-data-pipeline-*",
        "arn:aws:s3:::aws-glue-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:/aws-glue/*"
    }
  ]
}
```

---

## Windows PowerShell Requirements

### Critical Rules for All Scripts

**REQUIREMENT 1: File Format**
- Extension: `.ps1` (NOT `.sh`, `.bash`, `.py`)
- Encoding: ASCII (NOT UTF-8, NOT UTF-16)
- Line endings: CRLF (Windows style)

**REQUIREMENT 2: No Unicode Characters**
- NO emojis or special symbols
- NO special quotes or dashes
- NO box-drawing characters
- USE standard ASCII: [OK], [ERROR], [WARN]

**REQUIREMENT 3: PowerShell Syntax**
- Use `$Variable` (NOT `${Variable}`)
- Use `@'...'@` for JSON here-strings (NOT `@"..."@`)
- Use `Set-Content -Encoding ASCII` for file writes
- Use `$LASTEXITCODE` for exit code checking

### Correct PowerShell Patterns

**Creating JSON Files:**
```powershell
# CORRECT - Single-quoted here-string prevents variable expansion
$jsonContent = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject"
    }
  ]
}
'@

# Write with ASCII encoding
Set-Content -Path "policy.json" -Value $jsonContent -Encoding ASCII
```

**AWS CLI Error Handling:**
```powershell
# CORRECT - Capture both stdout and stderr
$output = aws iam create-role --role-name MyRole --assume-role-policy-document file://policy.json 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Role created" -ForegroundColor Green
} elseif ($output -match "EntityAlreadyExists") {
    Write-Host "[WARN] Role already exists" -ForegroundColor Yellow
} else {
    Write-Host "[ERROR] Failed: $output" -ForegroundColor Red
    exit 1
}
```

**Colored Output (Windows-safe):**
```powershell
# CORRECT - ASCII symbols only
Write-Host "[OK] Success" -ForegroundColor Green
Write-Host "[ERROR] Failed" -ForegroundColor Red
Write-Host "[WARN] Warning" -ForegroundColor Yellow
Write-Host "[INFO] Information" -ForegroundColor Cyan
```

### Common Mistakes to Avoid

**MISTAKE 1: Double-Quoted Here-Strings**
```powershell
# WRONG - Variables get expanded, breaks JSON
$bucketName = "my-bucket"
$json = @"
{
  "Resource": "arn:aws:s3:::$bucketName/*"
}
"@
# Result: Broken JSON because $bucketName expanded

# CORRECT - Single quotes prevent expansion
$json = @'
{
  "Resource": "arn:aws:s3:::my-bucket/*"
}
'@
```

**MISTAKE 2: Wrong File Encoding**
```powershell
# WRONG - Uses UTF-8 with BOM (breaks AWS CLI)
$json | Out-File -FilePath "policy.json"

# CORRECT - Uses ASCII
$json | Set-Content -Path "policy.json" -Encoding ASCII
```

**MISTAKE 3: Bash Syntax in PowerShell**
```powershell
# WRONG - This is bash, not PowerShell
export AWS_REGION=us-east-1
echo "Hello"

# CORRECT - PowerShell syntax
$env:AWS_REGION = "us-east-1"
Write-Host "Hello"
```

---

## Troubleshooting Common Issues

### Issue 1: "Role cannot be found" After Creation

**Symptom:**
```
NoSuchEntity: The role with name AWSGlueServiceRole-MarketData cannot be found.
```

**Cause:** IAM eventual consistency - changes take time to propagate

**Solution:**
```powershell
# Wait 30-60 seconds
Start-Sleep -Seconds 30

# Verify role exists
aws iam get-role --role-name AWSGlueServiceRole-MarketData
```

### Issue 2: "Access Denied" When Creating Role

**Symptom:**
```
AccessDenied: User is not authorized to perform iam:CreateRole
```

**Cause:** Current user lacks IAM permissions

**Solution:**
1. Log into AWS Console as root/admin user
2. Navigate to IAM > Users > prefect-market-data
3. Add temporary policy: `IAMFullAccess`
4. Run the script
5. Remove `IAMFullAccess` after completion (security best practice)

### Issue 3: Glue Job Fails with "Access Denied" on S3

**Symptom:**
```
AccessDeniedException: Access Denied (Service: Amazon S3)
```

**Cause:** Role's S3 policy doesn't match your bucket name

**Solution:**
```powershell
# 1. Check your actual bucket name
aws s3 ls | findstr market

# 2. If bucket name doesn't match pattern 'market-data-pipeline-*', 
#    update the role's inline policy:

# Create updated policy
$s3Policy = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::YOUR-ACTUAL-BUCKET-NAME/*",
        "arn:aws:s3:::aws-glue-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::YOUR-ACTUAL-BUCKET-NAME",
        "arn:aws:s3:::aws-glue-*"
      ]
    }
  ]
}
'@

Set-Content -Path "s3-policy-updated.json" -Value $s3Policy -Encoding ASCII

# 3. Update the role policy
aws iam put-role-policy `
  --role-name AWSGlueServiceRole-MarketData `
  --policy-name MarketDataS3Access `
  --policy-document file://s3-policy-updated.json
```

### Issue 4: PowerShell Script Has Syntax Errors

**Symptom:**
```
Unexpected token ':' in expression or statement
```

**Cause:** Script contains Unicode characters or wrong encoding

**Solution:**
```powershell
# Check file encoding
Get-Content .\script.ps1 -Encoding Byte | Select-Object -First 3
# Should show ASCII bytes (< 128)

# Re-save with correct encoding
$content = Get-Content .\script.ps1 -Raw
Set-Content -Path .\script-fixed.ps1 -Value $content -Encoding ASCII
```

---

## Script Creation Guidelines

### Use the Template

All PowerShell scripts should follow this structure (see POWERSHELL_TEMPLATE.ps1):

```powershell
# Script Purpose
# Author: Nathan Low
# Date: YYYY-MM-DD

# Configuration
$RoleName = "AWSGlueServiceRole-MarketData"
$AccountId = "124909680453"

# Helper function for status messages
function Write-Status {
    param([string]$Message, [string]$Type = "Info")
    switch ($Type) {
        "Success" { Write-Host "[OK] $Message" -ForegroundColor Green }
        "Error"   { Write-Host "[ERROR] $Message" -ForegroundColor Red }
        "Warning" { Write-Host "[WARN] $Message" -ForegroundColor Yellow }
        default   { Write-Host "[INFO] $Message" -ForegroundColor Cyan }
    }
}

# Main logic
Write-Status "Starting script..." -Type "Info"

# Create JSON with single-quoted here-string
$jsonContent = @'
{
  "key": "value"
}
'@

# Save with ASCII encoding
Set-Content -Path "file.json" -Value $jsonContent -Encoding ASCII

# Execute AWS command with error handling
$output = aws iam create-role --role-name $RoleName --assume-role-policy-document file://file.json 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Status "Role created" -Type "Success"
} else {
    Write-Status "Failed: $output" -Type "Error"
    exit 1
}

# Cleanup
Remove-Item "file.json" -ErrorAction SilentlyContinue

Write-Status "Script complete" -Type "Success"
```

### JSON File Creation Pattern

**ALWAYS use this pattern for creating JSON files:**

```powershell
# Step 1: Define JSON using single-quoted here-string
$policyJson = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": "*"
    }
  ]
}
'@

# Step 2: Write to file with ASCII encoding
Set-Content -Path "policy.json" -Value $policyJson -Encoding ASCII -NoNewline

# Step 3: Use the file
aws iam put-role-policy --role-name MyRole --policy-name MyPolicy --policy-document file://policy.json

# Step 4: Clean up
Remove-Item "policy.json" -ErrorAction SilentlyContinue
```

---

## Validation Checklist

Before running ANY PowerShell script, verify:

### Encoding Check
```powershell
# Check if file is ASCII
Get-Content .\script.ps1 -Encoding Byte | Select-Object -First 100 | Where-Object { $_ -gt 127 }
# Should return nothing (all bytes < 128)
```

### Syntax Check
```powershell
# Parse the script without executing
$errors = $null
$null = [System.Management.Automation.PSParser]::Tokenize((Get-Content .\script.ps1 -Raw), [ref]$errors)
if ($errors.Count -gt 0) {
    Write-Host "Syntax errors found:" -ForegroundColor Red
    $errors | Format-Table
} else {
    Write-Host "No syntax errors" -ForegroundColor Green
}
```

### JSON Validation
```powershell
# Test JSON is valid
Get-Content .\policy.json -Raw | ConvertFrom-Json
# Should parse without errors
```

---

## Running the Glue Pipeline

### Pre-Flight Checklist

```powershell
# 1. Verify AWS credentials
aws sts get-caller-identity

# 2. Verify IAM role exists
aws iam get-role --role-name AWSGlueServiceRole-MarketData

# 3. Verify S3 bucket exists
aws s3 ls s3://market-data-pipeline-YOUR-BUCKET/

# 4. Check Prefect is installed
prefect version
```

### Run the Flow

```powershell
# Navigate to project directory
cd C:\Git\realtime-stock-data\market-data-spark-emr

# Run the Prefect flow
python flows\glue_pipeline.py
```

### Expected Output

```
[INFO] Flow run 'market-data-pipeline' - Starting flow
[INFO] Task run 'create_glue_job' - Creating Glue job: market-data-daily-analytics
[INFO] Task run 'create_glue_job' - Job created successfully
[INFO] Task run 'start_glue_job' - Starting job run
[INFO] Task run 'start_glue_job' - Job run ID: jr_abc123...
[INFO] Task run 'wait_for_glue_job' - Job status: RUNNING
[INFO] Task run 'wait_for_glue_job' - Job status: RUNNING
[INFO] Task run 'wait_for_glue_job' - Job status: SUCCEEDED
[INFO] Flow run 'market-data-pipeline' - Finished successfully
```

### Timeline

- **Job creation:** 2-5 seconds
- **Job start:** 5-10 seconds
- **Container spin-up:** 60-90 seconds (first run)
- **Execution:** 30-60 seconds (depends on data)
- **Total:** 2-3 minutes

---

## Monitoring & Debugging

### Monitor Glue Job Progress

**Via AWS CLI:**
```powershell
# Get job run status
aws glue get-job-run `
  --job-name market-data-daily-analytics `
  --run-id jr_YOUR_RUN_ID `
  --query 'JobRun.[JobName,JobRunState,ErrorMessage]' `
  --output table
```

**Via AWS Console:**
1. Open: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=jobs
2. Click on job: `market-data-daily-analytics`
3. View run history and logs

### CloudWatch Logs

**View logs in real-time:**
```powershell
# Tail the output logs
aws logs tail /aws-glue/jobs/output --follow --filter-pattern "market-data-daily-analytics"

# View error logs
aws logs tail /aws-glue/jobs/error --follow --filter-pattern "market-data-daily-analytics"
```

**Via AWS Console:**
1. Open: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups
2. Navigate to: `/aws-glue/jobs/output` or `/aws-glue/jobs/error`
3. Find log stream with your job run ID

### Check S3 Output

```powershell
# List processed data
aws s3 ls s3://market-data-pipeline-YOUR-BUCKET/processed/ --recursive

# Download sample file
aws s3 cp s3://market-data-pipeline-YOUR-BUCKET/processed/date=2026-02-07/part-00000.parquet .\sample.parquet
```

### Common Error Messages

**Error:** "Job failed with exception: Access Denied"
**Fix:** Check IAM role has S3 permissions (see Issue 3 above)

**Error:** "Job failed: Script not found"
**Fix:** Verify S3 path to Glue script in job definition

**Error:** "Job failed: Module not found"
**Fix:** Add required Python packages to Glue job's `--additional-python-modules` parameter

---

## Best Practices

### IAM Security

1. **Use service-specific roles** - Don't give broad permissions
2. **Principle of least privilege** - Only grant necessary permissions
3. **Separate roles for dev/prod** - `AWSGlueServiceRole-MarketData-Dev` vs `-Prod`
4. **Regular audits** - Review IAM policies quarterly

### Script Management

1. **Version control** - Keep all `.ps1` scripts in Git
2. **Test before production** - Always test scripts in dev environment
3. **Document changes** - Add comments explaining non-obvious logic
4. **Error handling** - Always check `$LASTEXITCODE` after AWS commands

### Cost Optimization

1. **Use Glue DPU wisely** - Start with 2 DPUs, scale up if needed
2. **Enable job metrics** - Monitor actual DPU usage
3. **Set job timeouts** - Prevent runaway jobs
4. **Use development endpoints** - For testing before production runs

### Monitoring

1. **Set up CloudWatch alarms** - Alert on job failures
2. **Track execution time** - Identify performance issues
3. **Monitor costs** - AWS Cost Explorer for Glue spending
4. **Log everything** - Enable detailed logging for debugging

---

## Quick Reference

### Essential AWS Commands

```powershell
# IAM
aws iam get-role --role-name AWSGlueServiceRole-MarketData
aws iam list-attached-role-policies --role-name AWSGlueServiceRole-MarketData

# Glue
aws glue get-job --job-name market-data-daily-analytics
aws glue start-job-run --job-name market-data-daily-analytics
aws glue get-job-run --job-name market-data-daily-analytics --run-id jr_XXX

# S3
aws s3 ls s3://market-data-pipeline-YOUR-BUCKET/
aws s3 cp local-file.txt s3://market-data-pipeline-YOUR-BUCKET/

# CloudWatch Logs
aws logs tail /aws-glue/jobs/output --follow
aws logs describe-log-streams --log-group-name /aws-glue/jobs/output
```

### Troubleshooting Commands

```powershell
# Check AWS credentials
aws sts get-caller-identity

# Test IAM permissions
aws iam simulate-principal-policy `
  --policy-source-arn arn:aws:iam::124909680453:role/service-role/AWSGlueServiceRole-MarketData `
  --action-names s3:GetObject s3:PutObject glue:GetJob

# Verify S3 access
aws s3api head-bucket --bucket market-data-pipeline-YOUR-BUCKET

# Check Glue job configuration
aws glue get-job --job-name market-data-daily-analytics --query 'Job.[Name,Role,Command.ScriptLocation]'
```

---

## Resources

### Documentation
- AWS Glue Developer Guide: https://docs.aws.amazon.com/glue/
- Prefect 3.0 Docs: https://docs.prefect.io/
- PowerShell Reference: https://learn.microsoft.com/en-us/powershell/

### Scripts in This Project
- `create-glue-role-v2.ps1` - IAM role setup
- `POWERSHELL_TEMPLATE.ps1` - Standard script template
- `WINDOWS_VALIDATION_CHECKLIST.md` - Quality checklist

### Support
- AWS Support: https://console.aws.amazon.com/support/
- Prefect Community: https://prefect.io/slack

---

## Changelog

### 2026-02-07
- Created initial CLAUDE.md documentation
- Documented IAM role setup process
- Added Windows PowerShell requirements
- Added troubleshooting guide
- Added validation checklist
- Emphasized ASCII encoding requirements for Windows compatibility

---

**Last Updated:** February 7, 2026  
**Maintained By:** Nathan Low  
**Environment:** Windows PowerShell / AWS Glue / Prefect 3.0