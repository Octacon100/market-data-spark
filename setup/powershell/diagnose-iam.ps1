# Diagnostic Script - Check IAM Status
# This will show us what roles and policies actually exist

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "AWS IAM Diagnostic Check" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$RoleName = "AWSGlueServiceRole-MarketData"

# Check 1: Who am I?
Write-Host "[1] Current AWS User:" -ForegroundColor Yellow
aws sts get-caller-identity --output table
Write-Host ""

# Check 2: Does the role exist?
Write-Host "[2] Searching for Glue role..." -ForegroundColor Yellow
try {
    $roleInfo = aws iam get-role --role-name $RoleName 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✅ Role EXISTS!" -ForegroundColor Green
        Write-Host ""
        Write-Host "Role Details:" -ForegroundColor Cyan
        aws iam get-role --role-name $RoleName --query 'Role.[RoleName,Arn,CreateDate]' --output table
    }
}
catch {
    Write-Host "  ❌ Role DOES NOT EXIST" -ForegroundColor Red
}
Write-Host ""

# Check 3: List all roles with "Glue" in the name
Write-Host "[3] All roles containing 'Glue':" -ForegroundColor Yellow
aws iam list-roles --query "Roles[?contains(RoleName, 'Glue')].[RoleName,Arn]" --output table
Write-Host ""

# Check 4: What policies are attached to the role (if it exists)?
Write-Host "[4] Checking attached policies..." -ForegroundColor Yellow
try {
    aws iam list-attached-role-policies --role-name $RoleName --output table 2>$null
    Write-Host ""
    Write-Host "Inline policies:" -ForegroundColor Cyan
    aws iam list-role-policies --role-name $RoleName --output table 2>$null
}
catch {
    Write-Host "  (Role doesn't exist, skipping policy check)" -ForegroundColor Gray
}
Write-Host ""

# Check 5: Trust relationship
Write-Host "[5] Role trust policy:" -ForegroundColor Yellow
try {
    aws iam get-role --role-name $RoleName --query 'Role.AssumeRolePolicyDocument' --output json 2>$null
}
catch {
    Write-Host "  (Role doesn't exist)" -ForegroundColor Gray
}
Write-Host ""

# Check 6: List roles in service-role path
Write-Host "[6] Roles in /service-role/ path:" -ForegroundColor Yellow
aws iam list-roles --path-prefix /service-role/ --query 'Roles[*].[RoleName,Arn]' --output table
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Diagnostic Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
