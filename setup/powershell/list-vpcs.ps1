# List Available VPCs and Subnets
# Helps you see what networking resources you have

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "AWS VPC and Subnet Inspector" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if AWS CLI is available
$awsVersion = & aws --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] AWS CLI not found" -ForegroundColor Red
    exit 1
}

# Check credentials
$identity = & aws sts get-caller-identity 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] AWS credentials not configured" -ForegroundColor Red
    exit 1
}

Write-Host "Authenticated as:" -ForegroundColor Yellow
$identity | ConvertFrom-Json | Format-Table -Property Account, UserId, Arn
Write-Host ""

# Get all VPCs
Write-Host "Available VPCs:" -ForegroundColor Yellow
Write-Host "----------------------------------------" -ForegroundColor Gray

$vpcsJson = & aws ec2 describe-vpcs --output json
$vpcs = $vpcsJson | ConvertFrom-Json

if ($vpcs.Vpcs.Count -eq 0) {
    Write-Host "[ERROR] No VPCs found!" -ForegroundColor Red
    Write-Host "You need at least one VPC to run EMR clusters." -ForegroundColor Yellow
    exit 1
}

$vpcNumber = 1
foreach ($vpc in $vpcs.Vpcs) {
    $isDefault = if ($vpc.IsDefault) { " [DEFAULT]" } else { "" }
    
    Write-Host "`nVPC #$vpcNumber$isDefault" -ForegroundColor Cyan
    Write-Host "  VPC ID:      $($vpc.VpcId)" -ForegroundColor White
    Write-Host "  CIDR Block:  $($vpc.CidrBlock)" -ForegroundColor Gray
    Write-Host "  State:       $($vpc.State)" -ForegroundColor Gray
    
    # Get name tag if exists
    $nameTag = $vpc.Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -First 1
    if ($nameTag) {
        Write-Host "  Name:        $($nameTag.Value)" -ForegroundColor Gray
    }
    
    # Get subnets in this VPC
    $subnetsJson = & aws ec2 describe-subnets --filters "Name=vpc-id,Values=$($vpc.VpcId)" --output json
    $subnets = ($subnetsJson | ConvertFrom-Json).Subnets
    
    if ($subnets.Count -gt 0) {
        Write-Host "  Subnets:     $($subnets.Count) available" -ForegroundColor Green
        
        foreach ($subnet in $subnets) {
            $subnetName = ($subnet.Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -First 1).Value
            if ($subnetName) {
                Write-Host "    - $($subnet.SubnetId) ($subnetName) - $($subnet.AvailabilityZone)" -ForegroundColor Gray
            }
            else {
                Write-Host "    - $($subnet.SubnetId) - $($subnet.AvailabilityZone)" -ForegroundColor Gray
            }
        }
    }
    else {
        Write-Host "  Subnets:     [WARNING] No subnets!" -ForegroundColor Yellow
    }
    
    $vpcNumber++
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total VPCs:    $($vpcs.Vpcs.Count)" -ForegroundColor White
Write-Host "VPC Limit:     5 per region (default)" -ForegroundColor Gray
Write-Host ""

# Check if we have a default VPC
$hasDefault = $vpcs.Vpcs | Where-Object { $_.IsDefault -eq $true }
if ($hasDefault) {
    Write-Host "[OK] You have a default VPC" -ForegroundColor Green
    Write-Host "     The pipeline will use: $($hasDefault.VpcId)" -ForegroundColor Gray
}
else {
    Write-Host "[WARNING] No default VPC found" -ForegroundColor Yellow
    Write-Host "     The pipeline will use the first available VPC" -ForegroundColor Gray
    Write-Host "     VPC selected: $($vpcs.Vpcs[0].VpcId)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Recommendations" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if (-not $hasDefault) {
    Write-Host ""
    Write-Host "Option 1: Delete an unused VPC to free up space" -ForegroundColor Yellow
    Write-Host "  1. Identify unused VPC from list above" -ForegroundColor Gray
    Write-Host "  2. aws ec2 delete-vpc --vpc-id vpc-xxxxx" -ForegroundColor Gray
    Write-Host "  3. aws ec2 create-default-vpc" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Option 2: Use existing VPC (recommended)" -ForegroundColor Yellow
    Write-Host "  The pipeline will automatically use an available VPC" -ForegroundColor Gray
    Write-Host "  No action needed - just run the pipeline!" -ForegroundColor Green
}
else {
    Write-Host ""
    Write-Host "[OK] You're all set!" -ForegroundColor Green
    Write-Host "     Just run: python flows\market_data_with_spark.py" -ForegroundColor White
}

Write-Host ""
