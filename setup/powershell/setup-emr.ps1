# Setup EMR for Market Data Pipeline
# PowerShell version for Windows users

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "EMR Setup Script for Windows" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if AWS CLI is installed
Write-Host "Checking AWS CLI..." -ForegroundColor Yellow
$awsVersion = & aws --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] AWS CLI installed: $awsVersion" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] AWS CLI not found. Install from: https://aws.amazon.com/cli/" -ForegroundColor Red
    exit 1
}

# Check AWS credentials
Write-Host "`nChecking AWS credentials..." -ForegroundColor Yellow
$identity = & aws sts get-caller-identity 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] AWS credentials configured" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] AWS credentials not configured. Run: aws configure" -ForegroundColor Red
    exit 1
}

# Load environment variables from .env file
Write-Host "`nLoading environment variables from .env..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "  Set $name" -ForegroundColor Gray
        }
    }
    Write-Host "  [OK] Environment variables loaded" -ForegroundColor Green
}
else {
    Write-Host "  [ERROR] .env file not found. Copy from .env.example" -ForegroundColor Red
    exit 1
}

# Get S3 bucket name from environment
$S3_BUCKET = $env:S3_BUCKET
if (-not $S3_BUCKET) {
    Write-Host "  [ERROR] S3_BUCKET not set in .env file" -ForegroundColor Red
    exit 1
}

# Create S3 bucket if it doesn't exist
Write-Host "`nChecking S3 bucket: $S3_BUCKET" -ForegroundColor Yellow
$bucketExists = & aws s3 ls "s3://$S3_BUCKET" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Creating S3 bucket: $S3_BUCKET..." -ForegroundColor Yellow
    & aws s3 mb "s3://$S3_BUCKET"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] S3 bucket created successfully" -ForegroundColor Green
    }
    else {
        Write-Host "  [ERROR] Failed to create S3 bucket" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "  [OK] S3 bucket exists" -ForegroundColor Green
}

# Create EMR default roles
Write-Host "`nCreating EMR default roles..." -ForegroundColor Yellow
$rolesOutput = & aws emr create-default-roles 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] EMR default roles created" -ForegroundColor Green
}
elseif ($rolesOutput -like "*already exists*") {
    Write-Host "  [OK] EMR default roles already exist" -ForegroundColor Green
}
else {
    Write-Host "  [WARNING] Could not verify EMR roles (may already exist)" -ForegroundColor Yellow
}

# Create EC2 key pair if specified
$EMR_KEY_PAIR = $env:EMR_KEY_PAIR
if ($EMR_KEY_PAIR) {
    Write-Host "`nChecking EC2 key pair: $EMR_KEY_PAIR" -ForegroundColor Yellow
    $keyExists = & aws ec2 describe-key-pairs --key-names "$EMR_KEY_PAIR" 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Creating EC2 key pair: $EMR_KEY_PAIR..." -ForegroundColor Yellow
        
        # Create key pair and save to file
        $keyMaterial = & aws ec2 create-key-pair --key-name "$EMR_KEY_PAIR" --query "KeyMaterial" --output text
        
        if ($LASTEXITCODE -eq 0) {
            $keyFile = "$EMR_KEY_PAIR.pem"
            Set-Content -Path $keyFile -Value $keyMaterial
            Write-Host "  [OK] Key pair created and saved to: $keyFile" -ForegroundColor Green
            Write-Host "  [WARNING] Keep this file safe! You'll need it to SSH into EMR clusters" -ForegroundColor Yellow
        }
        else {
            Write-Host "  [ERROR] Failed to create key pair" -ForegroundColor Red
        }
    }
    else {
        Write-Host "  [OK] EC2 key pair exists" -ForegroundColor Green
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Upload Spark scripts: python setup\upload_spark_scripts.py" -ForegroundColor White
Write-Host "  2. Start Prefect worker: prefect worker start --pool local-laptop" -ForegroundColor White
Write-Host "  3. Run pipeline: python flows\market_data_with_spark.py" -ForegroundColor White
Write-Host ""
