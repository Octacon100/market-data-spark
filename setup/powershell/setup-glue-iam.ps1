# Complete Glue IAM Setup Script (PowerShell)
# Run this once to set up all permissions for Glue pipeline

# Configuration
$UserName = "prefect-market-data"
$RoleName = "AWSGlueServiceRole-MarketData"
$AccountId = "124909680453"
$BucketPrefix = "market-data-"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "AWS Glue IAM Setup" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "User: $UserName" -ForegroundColor White
Write-Host "Role: $RoleName" -ForegroundColor White
Write-Host "Account: $AccountId" -ForegroundColor White
Write-Host ""

# Step 1: Create Glue Service Role Trust Policy
Write-Host "[1/5] Creating Glue service role trust policy..." -ForegroundColor Yellow

$GlueTrustPolicy = @"
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
"@

$GlueTrustPolicy | Out-File -FilePath "$env:TEMP\glue-trust-policy.json" -Encoding ASCII

# Step 2: Create Glue Service Role
Write-Host "[2/5] Creating Glue service role..." -ForegroundColor Yellow

try {
    & aws iam get-role --role-name $RoleName 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] Role already exists" -ForegroundColor Green
    }
} catch {
    & aws iam create-role `
        --role-name $RoleName `
        --assume-role-policy-document "file://$env:TEMP\glue-trust-policy.json" `
        --description "Service role for Glue jobs in market data pipeline"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] Role created" -ForegroundColor Green
    } else {
        Write-Host "  [ERROR] Failed to create role" -ForegroundColor Red
    }
}

# Step 3: Attach AWS Managed Glue Policy to Service Role
Write-Host "[3/5] Attaching managed Glue policy to service role..." -ForegroundColor Yellow

& aws iam attach-role-policy `
    --role-name $RoleName `
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" 2>$null

if ($LASTEXITCODE -eq 0 -or $LASTEXITCODE -eq 254) {
    Write-Host "  [OK] Policy attached" -ForegroundColor Green
}

# Step 4: Add S3 Access to Service Role
Write-Host "[4/5] Adding S3 access to service role..." -ForegroundColor Yellow

$GlueS3Policy = @"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ObjectAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::${BucketPrefix}*/*"
        },
        {
            "Sid": "S3BucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::${BucketPrefix}*"
        }
    ]
}
"@

$GlueS3Policy | Out-File -FilePath "$env:TEMP\glue-s3-policy.json" -Encoding ASCII

& aws iam put-role-policy `
    --role-name $RoleName `
    --policy-name "GlueS3Access" `
    --policy-document "file://$env:TEMP\glue-s3-policy.json"

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] S3 policy attached" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Failed to attach S3 policy" -ForegroundColor Red
}

# Step 5: Add Glue Permissions to User
Write-Host "[5/5] Adding Glue permissions to user..." -ForegroundColor Yellow

$UserGluePolicy = @"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueJobManagement",
            "Effect": "Allow",
            "Action": [
                "glue:CreateJob",
                "glue:DeleteJob",
                "glue:GetJob",
                "glue:GetJobs",
                "glue:ListJobs",
                "glue:UpdateJob",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun"
            ],
            "Resource": "*"
        },
        {
            "Sid": "GluePassRole",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::${AccountId}:role/${RoleName}",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "glue.amazonaws.com"
                }
            }
        },
        {
            "Sid": "CloudWatchLogsRead",
            "Effect": "Allow",
            "Action": [
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": "arn:aws:logs:*:${AccountId}:log-group:/aws-glue/*"
        },
        {
            "Sid": "DiagnosticPermissions",
            "Effect": "Allow",
            "Action": [
                "iam:ListAttachedUserPolicies",
                "iam:ListUserPolicies",
                "servicequotas:GetServiceQuota"
            ],
            "Resource": "*"
        }
    ]
}
"@

$UserGluePolicy | Out-File -FilePath "$env:TEMP\user-glue-policy.json" -Encoding ASCII

& aws iam put-user-policy `
    --user-name $UserName `
    --policy-name "GlueUserPolicy" `
    --policy-document "file://$env:TEMP\user-glue-policy.json"

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] User policy attached" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Failed to attach user policy" -ForegroundColor Red
}

# Cleanup temp files
Remove-Item -Path "$env:TEMP\glue-trust-policy.json" -ErrorAction SilentlyContinue
Remove-Item -Path "$env:TEMP\glue-s3-policy.json" -ErrorAction SilentlyContinue
Remove-Item -Path "$env:TEMP\user-glue-policy.json" -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "[COMPLETE] Setup Complete!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Verification:" -ForegroundColor Yellow
Write-Host ""

# Verify role exists
& aws iam get-role --role-name $RoleName 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Glue service role exists" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Glue service role NOT found" -ForegroundColor Red
}

# Verify user policy exists
& aws iam get-user-policy --user-name $UserName --policy-name "GlueUserPolicy" 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] User Glue policy attached" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] User Glue policy NOT found" -ForegroundColor Red
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Run: python flows/market_data_with_glue.py" -ForegroundColor White
Write-Host "  2. Check: AWS Glue Console for job status" -ForegroundColor White
Write-Host ""
Write-Host "Done! [COMPLETE]" -ForegroundColor Green
