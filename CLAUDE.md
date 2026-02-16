# Market Data Pipeline - Claude Code Instructions

**Author:** Nathan Low
**Environment:** Windows PowerShell | AWS us-east-1 | Account 124909680453

## Rules for All Code Generation

- Scripts must be `.ps1` with ASCII encoding and CRLF line endings
- NO Unicode/emojis — use [OK], [ERROR], [WARN], [INFO]
- Use `@'...'@` (single-quoted here-strings) for JSON, never `@"..."@`
- Use `Set-Content -Encoding ASCII` for file writes, never `Out-File`
- Use `$LASTEXITCODE` for AWS CLI error checking
- Capture errors with `2>&1`
- Use `Write-Host` with `-ForegroundColor` for colored output, never `echo`

## Key Resources

- **IAM Role:** `AWSGlueServiceRole-MarketData`
- **Role ARN:** `arn:aws:iam::124909680453:role/service-role/AWSGlueServiceRole-MarketData`
- **S3 Pattern:** `market-data-pipeline-*`
- **IAM User:** `prefect-market-data`
- **Stack:** Prefect 3.0, Spark 3.3+, AWS Glue

## Project Structure

- `flows/` — Prefect flows (EMR + Glue variants)
- `spark_jobs/` — Spark analytics scripts (daily_analytics, ml_features, volatility_metrics)
- `setup/powershell/` — AWS setup scripts (.ps1)

## Reference Docs

- See `RUNBOOK.md` for detailed IAM setup, troubleshooting, monitoring, and script templates
