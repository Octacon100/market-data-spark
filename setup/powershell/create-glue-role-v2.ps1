# Simple Glue Role Creation Script
# Creates the role step-by-step with verification

Write-Host "Creating AWS Glue Service Role" -ForegroundColor Cyan
Write-Host "===============================" -ForegroundColor Cyan
Write-Host ""

$RoleName = "AWSGlueServiceRole-MarketData"
$AccountId = "124909680453"

# Step 1: Create trust policy document
Write-Host "Step 1: Creating trust policy..." -ForegroundColor Yellow

$TrustPolicyJson = @'
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
'@

Set-Content -Path "trust-policy.json" -Value $TrustPolicyJson -Encoding ASCII
Write-Host "  [OK] Trust policy created" -ForegroundColor Green
Write-Host ""

# Step 2: Create the role
Write-Host "Step 2: Creating IAM role..." -ForegroundColor Yellow

$createOutput = aws iam create-role --role-name $RoleName --path /service-role/ --assume-role-policy-document file://trust-policy.json --description "Service role for AWS Glue" 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Role created successfully" -ForegroundColor Green
} elseif ($createOutput -match "EntityAlreadyExists") {
    Write-Host "  [WARN] Role already exists, updating trust policy" -ForegroundColor Yellow
    aws iam update-assume-role-policy --role-name $RoleName --policy-document file://trust-policy.json | Out-Null
    Write-Host "  [OK] Trust policy updated" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Failed to create role: $createOutput" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 3: Wait for propagation
Write-Host "Step 3: Waiting for IAM propagation (5 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5
Write-Host "  [OK] Done" -ForegroundColor Green
Write-Host ""

# Step 4: Verify role exists
Write-Host "Step 4: Verifying role exists..." -ForegroundColor Yellow
$verifyOutput = aws iam get-role --role-name $RoleName 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Role verified" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Role not found: $verifyOutput" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 5: Attach managed policy
Write-Host "Step 5: Attaching AWS managed Glue policy..." -ForegroundColor Yellow
aws iam attach-role-policy --role-name $RoleName --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Policy attached" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Policy might already be attached" -ForegroundColor Yellow
}
Write-Host ""

# Step 6: Create S3 policy
Write-Host "Step 6: Adding S3 and CloudWatch permissions..." -ForegroundColor Yellow

$S3PolicyJson = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::market-data-pipeline-*/*", "arn:aws:s3:::market-data-2025-11-28/*", "arn:aws:s3:::aws-glue-*/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::market-data-pipeline-*", "arn:aws:s3:::market-data-2025-11-28", "arn:aws:s3:::aws-glue-*"]
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:/aws-glue/*"
    }
  ]
}
'@

Set-Content -Path "s3-policy.json" -Value $S3PolicyJson -Encoding ASCII

aws iam put-role-policy --role-name $RoleName --policy-name MarketDataS3Access --policy-document file://s3-policy.json 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] S3 policy added" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Failed to add S3 policy" -ForegroundColor Red
}
Write-Host ""

# Cleanup
Remove-Item trust-policy.json -ErrorAction SilentlyContinue
Remove-Item s3-policy.json -ErrorAction SilentlyContinue

# Summary
Write-Host "========================================" -ForegroundColor Green
Write-Host "Setup Complete" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Role ARN: arn:aws:iam::$AccountId`:role/service-role/$RoleName" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Wait 30 seconds for full IAM propagation" -ForegroundColor White
Write-Host "  2. Re-run your Prefect flow" -ForegroundColor White
Write-Host ""
