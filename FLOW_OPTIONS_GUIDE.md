# Pipeline Flow Options Guide

## Available Flows

You now have **4 different ways** to run the pipeline, depending on what you want to demonstrate:

---

## Option 1: EMR Only (Original)

**File:** `market_data_with_spark.py`

**What it does:**
- Collects market data
- Runs Spark analytics on **EMR cluster**
- Shows infrastructure orchestration skills

**When to use:**
- Want to demonstrate EMR cluster management
- Need to show VPC/networking knowledge
- Portfolio piece focused on infrastructure depth

**Run:**
```bash
python flows/market_data_with_spark.py
```

**Cost:** ~$0.17 per run

---

## Option 2: Glue Only (New - Recommended)

**File:** `market_data_with_glue.py`

**What it does:**
- Collects market data
- Runs Spark analytics on **AWS Glue** (serverless)
- Shows modern serverless skills

**When to use:**
- Want simpler setup (no VPC issues!)
- Demonstrate serverless thinking
- Portfolio piece focused on pragmatic engineering

**Run:**
```bash
python flows/market_data_with_glue.py
```

**Cost:** ~$0.10-0.12 per run

---

## Option 3: Choose Your Backend (New - Most Flexible)

**File:** `market_data_spark.py`

**What it does:**
- Collects market data
- Lets you **choose** EMR or Glue at runtime
- Single codebase, multiple backends

**When to use:**
- Want to compare both backends
- Demonstrate architectural flexibility
- Interview prep (discuss tradeoffs)

**Run with Glue (default):**
```bash
python flows/market_data_spark.py glue
```

**Run with EMR:**
```bash
python flows/market_data_spark.py emr
```

**Cost:** Depends on backend chosen

---

## Option 4: Just Glue Analytics (Standalone)

**File:** `glue_tasks.py`

**What it does:**
- Assumes data already exists in S3
- Just runs Spark analytics on Glue
- No data collection phase

**When to use:**
- Data already collected
- Just want to run analytics
- Testing Glue jobs independently

**Run:**
```python
from glue_tasks import glue_analytics_flow
glue_analytics_flow()
```

**Cost:** ~$0.10-0.12 per run

---

## Flow Comparison

| Flow | Data Collection | Spark Backend | VPC Required | Startup Time | Cost/Run |
|------|-----------------|---------------|--------------|--------------|----------|
| `market_data_with_spark.py` | ✅ Yes | EMR | ✅ Yes | ~6 min | $0.17 |
| `market_data_with_glue.py` | ✅ Yes | Glue | ❌ No | ~1 min | $0.12 |
| `market_data_spark.py` | ✅ Yes | Your choice | Depends | Depends | Depends |
| `glue_tasks.py` (standalone) | ❌ No | Glue | ❌ No | ~1 min | $0.12 |

---

## Recommended Usage

### For Job Search / Portfolio:

**Best choice:** Use **`market_data_with_glue.py`**

**Why?**
- ✅ No VPC issues (simpler setup)
- ✅ Modern serverless approach
- ✅ 30% cheaper than EMR
- ✅ Faster startup
- ✅ Same Spark code

**But also keep EMR version** to show you understand both!

### For Interviews:

Use **`market_data_spark.py`** and say:

> "I built this with both EMR and Glue backends. I can show you either one - EMR demonstrates deeper infrastructure knowledge, while Glue shows modern serverless thinking. In production, I'd choose based on the workload pattern..."

Then discuss tradeoffs! 🎯

---

## Code Structure

All flows follow the same pattern:

```python
from prefect import flow
from market_data_flow import market_data_pipeline  # Data collection
from [emr_tasks|glue_tasks] import spark_flow      # Spark analytics

@flow
def main_pipeline():
    # Phase 1: Collect data
    data_results = market_data_pipeline()
    
    # Phase 2: Run Spark analytics
    spark_results = spark_analytics_flow()  # EMR or Glue
    
    # Phase 3: Return results
    return {'data': data_results, 'spark': spark_results}
```

**Same data collection code, different Spark backends!**

---

## Quick Reference

### Run EMR version:
```bash
cd flows
python market_data_with_spark.py
```

### Run Glue version:
```bash
cd flows
python market_data_with_glue.py
```

### Run with choice:
```bash
cd flows
python market_data_spark.py glue   # or 'emr'
```

### Just run analytics (data already exists):
```python
# In Python
from glue_tasks import glue_analytics_flow
glue_analytics_flow()
```

---

## What Changed from Original

**Original package:**
- Only had `market_data_with_spark.py` (EMR only)
- `glue_tasks.py` existed but wasn't integrated

**Now you have:**
- ✅ `market_data_with_spark.py` - EMR version (original)
- ✅ `market_data_with_glue.py` - Glue version (new)
- ✅ `market_data_spark.py` - Choose backend (new)
- ✅ `glue_tasks.py` - Standalone Glue analytics (already existed)

All using the **same Spark scripts** (`daily_analytics.py`, `ml_features.py`, `volatility_metrics.py`)!

---

## My Recommendation

### For Your Portfolio:

1. **Primary demo:** Use `market_data_with_glue.py`
   - Simpler to set up and run
   - No VPC headaches
   - Modern serverless approach

2. **Have available:** Keep `market_data_with_spark.py` (EMR version)
   - Shows you can handle complex infrastructure
   - Demonstrates cost optimization skills

3. **In README:** Explain you built both and why you chose Glue for the primary version

### In Interviews:

"I initially built this with EMR to learn cluster orchestration and VPC configuration. Then I also implemented it with Glue because in production, serverless is usually better for sporadic ETL workloads - simpler, faster startup, and 30% cheaper. Having both versions taught me when to use each approach."

**This shows:**
- ✅ Technical depth (EMR)
- ✅ Modern thinking (Glue)  
- ✅ Pragmatic decision-making
- ✅ Understanding of tradeoffs

Perfect for hedge fund interviews! 🎯
