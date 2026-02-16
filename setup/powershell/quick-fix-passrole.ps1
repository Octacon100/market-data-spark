# Quick Fix for PassRole Error (PowerShell)
# Adds iam:PassRole permission to your user

$UserName = "prefect-market-data"
$RoleName = "AWSGlueServiceRole-MarketData"
$AccountId = "124909680453"

Write-Host ""
Write-Host "Quick Fix: Adding PassRole Permission" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host ""

# Create policy document
$PassRolePolicy = @"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::${AccountId}:role/${RoleName}",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "glue.amazonaws.com"
                }
            }
        }
    ]
}
"@

Write-Host "Adding PassRole permission to user: $UserName" -ForegroundColor Yellow
Write-Host "For role: $RoleName" -ForegroundColor Yellow
Write-Host ""

# Save to temp file
$TempFile = "$env:TEMP\passrole-policy.json"
$PassRolePolicy | Out-File -FilePath $TempFile -Encoding ASCII

# Apply policy
& aws iam put-user-policy `
    --user-name $UserName `
    --policy-name "GluePassRolePolicy" `
    --policy-document "file://$TempFile"

# Check result
if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] PassRole permission added successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next step:" -ForegroundColor Cyan
    Write-Host "  python flows/market_data_with_glue.py" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host "[ERROR] Failed to add PassRole permission" -ForegroundColor Red
    Write-Host "Check your AWS credentials and try again" -ForegroundColor Yellow
    Write-Host ""
}

# Cleanup
Remove-Item -Path $TempFile -ErrorAction SilentlyContinue

# Verify
Write-Host "Verification:" -ForegroundColor Cyan
& aws iam get-user-policy --user-name $UserName --policy-name "GluePassRolePolicy" 2>$null | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Policy verified" -ForegroundColor Green
} else {
    Write-Host "  [WARNING] Could not verify policy" -ForegroundColor Yellow
}

Write-Host ""
