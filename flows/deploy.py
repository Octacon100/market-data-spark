"""
Deploy Market Data Pipeline with Spark
Creates Prefect deployments:
  - market-data-daily-with-spark: 6 PM ET daily (data collection + Spark analytics)
  - daily-morning-digest:         7 AM ET weekdays (email digest)
"""

from flows.market_data_with_spark import market_data_pipeline_with_spark
from flows.daily_digest import daily_digest_flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Data collection + Spark analytics - 6 PM ET daily
pipeline_deployment = Deployment.build_from_flow(
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

# Morning digest email - 7 AM ET weekdays only
digest_deployment = Deployment.build_from_flow(
    flow=daily_digest_flow,
    name="daily-morning-digest",
    schedule=CronSchedule(
        cron="0 7 * * 1-5",  # 7 AM ET, Mon-Fri only
        timezone="America/New_York"
    ),
    work_pool_name="local-laptop",
    description="Weekday morning email digest: buy signals, Reddit trends, price movers",
    tags=["production", "market-data", "digest", "email"]
)

if __name__ == "__main__":
    pipeline_deployment.apply()
    digest_deployment.apply()

    print("[OK] Deployments created successfully!")
    print("\nDeployments:")
    print("  market-data-daily-with-spark  - Daily at 6 PM ET")
    print("  daily-morning-digest          - Weekdays at 7 AM ET")
    print("\nWork pool: local-laptop")
    print("\nTo run:")
    print("  1. Start worker: prefect worker start --pool local-laptop")
    print("  2. View at: https://app.prefect.cloud")
