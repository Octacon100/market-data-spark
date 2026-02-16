# Quick Start Script for Market Data Pipeline
# PowerShell version for Windows users
# Run this after setting up .env file

param(
    [switch]$SkipInstall,
    [switch]$SkipAWS,
    [switch]$SkipPrefect
)

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "                                                                " -ForegroundColor Cyan
Write-Host "  Market Data Pipeline with Spark on EMR - Quick Start         " -ForegroundColor Cyan
Write-Host "                                                                " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Function to check if command exists
function Test-Command {
    param($Command)
    try {
        if (Get-Command $Command -ErrorAction Stop) {
            return $true
        }
    }
    catch {
        return $false
    }
}

# Step 1: Check prerequisites
Write-Host "Step 1: Checking Prerequisites" -ForegroundColor Yellow
Write-Host "-------------------------------------------------------------" -ForegroundColor Gray

$prerequisites = @{
    "Python" = @{
        Command = "python"
        Test = { python --version }
        Install = "https://www.python.org/downloads/"
    }
    "AWS CLI" = @{
        Command = "aws"
        Test = { aws --version }
        Install = "https://aws.amazon.com/cli/"
    }
    "Git" = @{
        Command = "git"
        Test = { git --version }
        Install = "https://git-scm.com/download/win"
    }
}

$allPrereqsMet = $true

foreach ($prereq in $prerequisites.GetEnumerator()) {
    $name = $prereq.Key
    $command = $prereq.Value.Command
    
    Write-Host "  Checking $name..." -NoNewline
    
    if (Test-Command $command) {
        try {
            $version = & $prereq.Value.Test 2>&1
            Write-Host " [OK]" -ForegroundColor Green
            Write-Host "    Version: $version" -ForegroundColor Gray
        }
        catch {
            Write-Host " [OK] (installed)" -ForegroundColor Green
        }
    }
    else {
        Write-Host " [ERROR]" -ForegroundColor Red
        Write-Host "    Install from: $($prereq.Value.Install)" -ForegroundColor Yellow
        $allPrereqsMet = $false
    }
}

if (-not $allPrereqsMet) {
    Write-Host "`n[ERROR] Please install missing prerequisites and try again" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 2: Install Python packages
if (-not $SkipInstall) {
    Write-Host "Step 2: Installing Python Packages" -ForegroundColor Yellow
    Write-Host "-------------------------------------------------------------" -ForegroundColor Gray
    
    if (Test-Path "requirements.txt") {
        Write-Host "  Installing from requirements.txt..." -ForegroundColor White
        pip install -r requirements.txt
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK] Packages installed successfully" -ForegroundColor Green
        }
        else {
            Write-Host "  [ERROR] Package installation failed" -ForegroundColor Red
            exit 1
        }
    }
    else {
        Write-Host "  [ERROR] requirements.txt not found" -ForegroundColor Red
        exit 1
    }
    Write-Host ""
}

# Step 3: Check .env file
Write-Host "Step 3: Checking Configuration" -ForegroundColor Yellow
Write-Host "-------------------------------------------------------------" -ForegroundColor Gray

if (-not (Test-Path ".env")) {
    Write-Host "  [ERROR] .env file not found" -ForegroundColor Red
    
    if (Test-Path ".env.example") {
        Write-Host "  Creating .env from template..." -ForegroundColor Yellow
        Copy-Item ".env.example" ".env"
        Write-Host "  [OK] Created .env file" -ForegroundColor Green
        Write-Host ""
        Write-Host "  [WARNING] IMPORTANT: Edit .env file with your credentials:" -ForegroundColor Yellow
        Write-Host "    - ALPHA_VANTAGE_API_KEY" -ForegroundColor White
        Write-Host "    - S3_BUCKET" -ForegroundColor White
        Write-Host "    - EMR_KEY_PAIR" -ForegroundColor White
        Write-Host ""
        Write-Host "  Opening .env in notepad..." -ForegroundColor Yellow
        Start-Process notepad.exe ".env"
        Write-Host ""
        Read-Host "  Press Enter after saving your changes"
    }
    else {
        Write-Host "  [ERROR] .env.example not found" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "  [OK] .env file exists" -ForegroundColor Green
}

# Load environment variables
Get-Content ".env" | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

# Verify critical variables
$criticalVars = @("ALPHA_VANTAGE_API_KEY", "S3_BUCKET")
$missingVars = @()

foreach ($var in $criticalVars) {
    $value = [Environment]::GetEnvironmentVariable($var)
    if (-not $value -or $value -eq "your_key_here" -or $value -eq "your-bucket-name") {
        $missingVars += $var
    }
}

if ($missingVars.Count -gt 0) {
    Write-Host ""
    Write-Host "  [ERROR] Missing required variables in .env:" -ForegroundColor Red
    foreach ($var in $missingVars) {
        Write-Host "    - $var" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "  Edit .env file and run this script again" -ForegroundColor Yellow
    exit 1
}

Write-Host "  [OK] Configuration looks good" -ForegroundColor Green
Write-Host ""

# Step 4: AWS Setup
if (-not $SkipAWS) {
    Write-Host "Step 4: AWS Setup" -ForegroundColor Yellow
    Write-Host "-------------------------------------------------------------" -ForegroundColor Gray
    
    # Check AWS credentials
    Write-Host "  Checking AWS credentials..." -NoNewline
    $identity = & aws sts get-caller-identity 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host " [OK]" -ForegroundColor Green
    }
    else {
        Write-Host " [ERROR]" -ForegroundColor Red
        Write-Host "  Run: aws configure" -ForegroundColor Yellow
        Write-Host "  Then run this script again" -ForegroundColor Yellow
        exit 1
    }
    
    # Run EMR setup script
    if (Test-Path "setup\powershell\setup-emr.ps1") {
        Write-Host "  Running EMR setup..." -ForegroundColor White
        & "setup\powershell\setup-emr.ps1"
    }
    
    Write-Host ""
}

# Step 5: Prefect Setup
if (-not $SkipPrefect) {
    Write-Host "Step 5: Prefect Setup" -ForegroundColor Yellow
    Write-Host "-------------------------------------------------------------" -ForegroundColor Gray
    
    Write-Host "  Checking Prefect Cloud connection..." -NoNewline
    $prefectServer = prefect config view 2>&1
    
    if ($prefectServer -match "api.prefect.cloud") {
        Write-Host " [OK]" -ForegroundColor Green
    }
    else {
        Write-Host " [ERROR]" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Login to Prefect Cloud:" -ForegroundColor Yellow
        Write-Host "    prefect cloud login" -ForegroundColor White
        Write-Host ""
        Read-Host "  Press Enter after logging in"
    }
    
    # Check work pool
    Write-Host "  Checking work pool..." -NoNewline
    $workPools = prefect work-pool ls 2>&1
    
    if ($workPools -match "local-laptop") {
        Write-Host " [OK]" -ForegroundColor Green
    }
    else {
        Write-Host " creating..." -ForegroundColor Yellow
        prefect work-pool create "local-laptop" --type process
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK] Work pool created" -ForegroundColor Green
        }
        else {
            Write-Host "  [ERROR] Failed to create work pool" -ForegroundColor Red
        }
    }
    
    Write-Host ""
}

# Step 6: Upload Spark Scripts
Write-Host "Step 6: Upload Spark Scripts to S3" -ForegroundColor Yellow
Write-Host "-------------------------------------------------------------" -ForegroundColor Gray

if (Test-Path "setup\upload_spark_scripts.py") {
    Write-Host "  Uploading Spark scripts to S3..." -ForegroundColor White
    python setup\upload_spark_scripts.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] Scripts uploaded successfully" -ForegroundColor Green
    }
    else {
        Write-Host "  [ERROR] Upload failed" -ForegroundColor Red
        Write-Host "  Check S3_BUCKET in .env file" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  [ERROR] upload_spark_scripts.py not found" -ForegroundColor Red
}

Write-Host ""

# Summary
Write-Host "================================================================" -ForegroundColor Green
Write-Host "                                                                " -ForegroundColor Green
Write-Host "  Setup Complete!                                              " -ForegroundColor Green
Write-Host "                                                                " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. Start Prefect Worker (Terminal 1):" -ForegroundColor White
Write-Host "     prefect worker start --pool local-laptop" -ForegroundColor Cyan
Write-Host ""
Write-Host "  2. Run the Pipeline (Terminal 2):" -ForegroundColor White
Write-Host "     python flows\market_data_with_spark.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "  3. Monitor Progress:" -ForegroundColor White
Write-Host "     - Prefect Cloud: https://app.prefect.cloud" -ForegroundColor Gray
Write-Host "     - EMR Console: https://console.aws.amazon.com/emr" -ForegroundColor Gray
Write-Host ""

Write-Host "For help, see:" -ForegroundColor Yellow
Write-Host "  - README.md - Complete documentation" -ForegroundColor Gray
Write-Host "  - QUICKSTART.md - Step-by-step guide" -ForegroundColor Gray
Write-Host ""