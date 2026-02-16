# Glue IAM Quick Reference

## Two Separate Things to Set Up:

### 1. Glue Service Role (What Glue uses)
**Name:** `AWSGlueServiceRole-MarketData`
**Purpose:** Glue jobs use this role to access S3, CloudWatch, etc.
**Needs:**
- Trust policy allowing `glue.amazonaws.com` to assume it
- AWS managed policy: `AWSGlueServiceRole`
- S3 access to your buckets

### 2. Your User Policy (What YOU need)
**Name:** `prefect-market-data` user
**Purpose:** You need permission to create/run Glue jobs and pass the role
**Needs:**
- `glue:CreateJob`, `glue:StartJobRun`, etc.
- `iam:PassRole` (to give the service role to Glue)

---

## Quick Setup (Copy/Paste)

### Option 1: Run the PowerShell Script (Recommended)
```powershell
.\setup-glue-iam.ps1
```

If you get an execution policy error:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\setup-glue-iam.ps1
```

### Option 2: Manual Commands

```bash
# 1. Create Glue service role
aws iam create-role \
    --role-name AWSGlueServiceRole-MarketData \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

# 2. Attach managed policy to service role
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MarketData \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# 3. Add Glue permissions to your user
aws iam put-user-policy \
    --user-name prefect-market-data \
    --policy-name GlueUserPolicy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:CreateJob",
                    "glue:GetJob",
                    "glue:StartJobRun",
                    "glue:GetJobRun"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": "arn:aws:iam::124909680453:role/AWSGlueServiceRole-MarketData",
                "Condition": {
                    "StringEquals": {
                        "iam:PassedToService": "glue.amazonaws.com"
                    }
                }
            }
        ]
    }'
```

---

## Verify

```bash
# Check if role exists
aws iam get-role --role-name AWSGlueServiceRole-MarketData

# Check if user has policy
aws iam get-user-policy \
    --user-name prefect-market-data \
    --policy-name GlueUserPolicy
```

---

## Test

```bash
python flows/market_data_with_glue.py
```

Should work! ✅

---

## What Each Does

```
You (prefect-market-data user)
    ↓
  Uses glue:CreateJob permission
    ↓
Creates Glue Job
    ↓
  Uses iam:PassRole permission  <-- This was missing!
    ↓
Passes AWSGlueServiceRole-MarketData to Glue
    ↓
Glue Job runs with that role
    ↓
  Uses s3:GetObject/PutObject (from service role)
    ↓
Reads/writes S3 data
```

---

## Common Errors

| Error | Fix |
|-------|-----|
| "not authorized to perform: iam:PassRole" | Add PassRole to user policy |
| "Role does not exist" | Create AWSGlueServiceRole-MarketData |
| "Access Denied" during job | Add S3 permissions to service role |
| "Cannot write logs" | Attach AWSGlueServiceRole managed policy |

---

## Files

- [COMPLETE_GLUE_IAM_SETUP.md](./COMPLETE_GLUE_IAM_SETUP.md) - Detailed guide
- [setup-glue-iam.ps1](./setup-glue-iam.ps1) - Automated PowerShell setup script
- [quick-fix-passrole.ps1](./quick-fix-passrole.ps1) - Quick PassRole fix (PowerShell)
- [FIX_PASSROLE_ERROR.md](./FIX_PASSROLE_ERROR.md) - PassRole explanation
