# Setup Amazon SES for Pipeline Email Notifications
# Author: Nathan Low
# Date: 2026-02-07

Write-Host "Setting up Amazon SES Email" -ForegroundColor Cyan
Write-Host "===========================" -ForegroundColor Cyan
Write-Host ""

$UserName = "prefect-market-data"
$EmailAddress = "youremailaddress@here.com"
$Region = "us-east-1"

# Step 1: Add SES permissions to IAM user
Write-Host "Step 1: Adding SES permissions to $UserName..." -ForegroundColor Yellow

$SesPolicyJson = @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ses:VerifyEmailIdentity",
        "ses:GetIdentityVerificationAttributes",
        "ses:SendEmail",
        "ses:SendRawEmail"
      ],
      "Resource": "*"
    }
  ]
}
'@

Set-Content -Path "ses-policy.json" -Value $SesPolicyJson -Encoding ASCII

$output = aws iam put-user-policy --user-name $UserName --policy-name SESEmailAccess --policy-document file://ses-policy.json 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] SES policy attached to $UserName" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Failed to attach policy: $output" -ForegroundColor Red
    Remove-Item ses-policy.json -ErrorAction SilentlyContinue
    exit 1
}
Write-Host ""

# Wait for IAM propagation
Write-Host "  [INFO] Waiting 30 seconds for IAM policy to propagate..." -ForegroundColor Cyan
Start-Sleep -Seconds 30
Write-Host "  [OK] Done waiting" -ForegroundColor Green
Write-Host ""

# Step 2: Verify email address
Write-Host "Step 2: Sending verification email to $EmailAddress..." -ForegroundColor Yellow

$output = aws ses verify-email-identity --email-address $EmailAddress --region $Region 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Verification email sent to $EmailAddress" -ForegroundColor Green
    Write-Host "  [INFO] Check your Gmail inbox and click the verification link" -ForegroundColor Cyan
} else {
    Write-Host "  [ERROR] Failed: $output" -ForegroundColor Red
    Remove-Item ses-policy.json -ErrorAction SilentlyContinue
    exit 1
}
Write-Host ""

# Step 3: Wait for user to verify
Write-Host "Step 3: Waiting for email verification..." -ForegroundColor Yellow
Write-Host "  [INFO] Please check your Gmail and click the verification link" -ForegroundColor Cyan
Write-Host "  [INFO] Press Enter after clicking the link..." -ForegroundColor Cyan
Read-Host

# Step 4: Check verification status
Write-Host "Step 4: Checking verification status..." -ForegroundColor Yellow

$output = aws ses get-identity-verification-attributes --identities $EmailAddress --region $Region --output json 2>&1

if ($LASTEXITCODE -eq 0) {
    if ($output -match "Success") {
        Write-Host "  [OK] Email verified successfully" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Email not yet verified. Try again in a minute." -ForegroundColor Yellow
        Write-Host "  [INFO] Run this to check: aws ses get-identity-verification-attributes --identities $EmailAddress --region $Region" -ForegroundColor Cyan
    }
} else {
    Write-Host "  [ERROR] Failed to check status: $output" -ForegroundColor Red
}
Write-Host ""

# Step 5: Send test email
Write-Host "Step 5: Sending test email..." -ForegroundColor Yellow

$output = aws ses send-email --from $EmailAddress --destination "ToAddresses=$EmailAddress" --message "Subject={Data='Market Data Pipeline - SES Test'},Body={Text={Data='SES is configured and working for your market data pipeline.'}}" --region $Region 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Test email sent to $EmailAddress" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Could not send test email: $output" -ForegroundColor Yellow
    Write-Host "  [INFO] This may fail if verification is still pending" -ForegroundColor Cyan
}
Write-Host ""

# Cleanup
Remove-Item ses-policy.json -ErrorAction SilentlyContinue

# Summary
Write-Host "========================================" -ForegroundColor Green
Write-Host "SES Setup Complete" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Email: $EmailAddress" -ForegroundColor White
Write-Host "Region: $Region" -ForegroundColor White
Write-Host "IAM User: $UserName" -ForegroundColor White
Write-Host ""
Write-Host "[INFO] Note: Your SES account is in sandbox mode." -ForegroundColor Cyan
Write-Host "[INFO] You can only send TO verified email addresses." -ForegroundColor Cyan
Write-Host "[INFO] Request production access in the AWS Console" -ForegroundColor Cyan
Write-Host "[INFO] to send to any address." -ForegroundColor Cyan
Write-Host ""
