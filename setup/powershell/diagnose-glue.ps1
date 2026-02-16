# Glue Access Diagnostic Script
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "AWS Glue Access Diagnostic" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Check 1: Account info
Write-Host "[1/5] Checking AWS account..." -ForegroundColor Yellow
& aws sts get-caller-identity
Write-Host ""

# Check 2: Region
Write-Host "[2/5] Checking AWS region..." -ForegroundColor Yellow
$Region = & aws configure get region
Write-Host "Region: $Region" -ForegroundColor White
Write-Host ""

# Check 3: Can we list Glue jobs?
Write-Host "[3/5] Testing Glue API access (list jobs)..." -ForegroundColor Yellow
& aws glue list-jobs --max-results 1 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Can access Glue API" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Cannot access Glue API" -ForegroundColor Red
}
Write-Host ""

# Check 4: Can we get Glue service quota?
Write-Host "[4/5] Checking Glue service availability..." -ForegroundColor Yellow
& aws service-quotas get-service-quota --service-code glue --quota-code L-B7A8EB7C 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Glue service is available" -ForegroundColor Green
} else {
    Write-Host "  [WARNING] Could not check Glue service quota" -ForegroundColor Yellow
}
Write-Host ""

# Check 5: List all IAM policies attached to user
Write-Host "[5/5] Checking user permissions..." -ForegroundColor Yellow
& aws iam list-attached-user-policies --user-name prefect-market-data 2>&1
Write-Host ""
& aws iam list-user-policies --user-name prefect-market-data 2>&1
Write-Host ""

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Diagnostic Complete" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "If you see 'AccessDeniedException' above, possible causes:" -ForegroundColor Yellow
Write-Host "  1. AWS Glue not enabled in your account" -ForegroundColor White
Write-Host "  2. Service Control Policy (SCP) blocking Glue" -ForegroundColor White
Write-Host "  3. Account requires admin to enable Glue" -ForegroundColor White
Write-Host "  4. Try AWS Console: https://console.aws.amazon.com/glue" -ForegroundColor White
Write-Host ""
