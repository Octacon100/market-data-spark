"""
Deploy Market Data Pipeline with Spark
Creates Prefect deployment with daily schedule
"""

from flows.market_data_with_spark import market_data_pipeline_with_spark
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Create deployment
deployment = Deployment.build_from_flow(
    flow=market_data_pipeline_with_spark,
    name="market-data-daily-with-spark",
    schedule=CronSchedule(
        cron="0 18 * * *",  # 6 PM ET daily
        timezone="America/New_York"
    ),
    work_pool_name="local-laptop",
    description="Daily market data collection + Spark analytics on EMR",
    tags=["production", "market-data", "spark", "emr"]
)

if __name__ == "__main__":
    # Apply deployment
    deployment.apply()
    
    print("✅ Deployment created successfully!")
    print("\nDeployment details:")
    print(f"  Name: market-data-daily-with-spark")
    print(f"  Schedule: Daily at 6 PM ET")
    print(f"  Work pool: local-laptop")
    print("\nTo run:")
    print("  1. Start worker: prefect worker start --pool local-laptop")
    print("  2. View at: https://app.prefect.cloud")
