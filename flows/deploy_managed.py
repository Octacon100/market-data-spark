from reddit_scanner import reddit_scanner_flow
from market_data_with_glue import market_data_pipeline_with_glue
from buy_signal_alerts import buy_signal_alert_flow
from daily_digest import daily_digest_flow
import argparse
import sys
from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository

"""
Deploy market data flows to Prefect Cloud (managed workers).

Deployments:
  reddit-ticker-scanner       - Every 6 hours
  market-data-pipeline-glue   - Daily at 6 PM ET
  buy-signal-alerts           - Daily at 6:30 PM ET
  daily-morning-digest        - Weekdays at 7 AM ET

Note: market-data-pipeline-glue calls buy-signal-alerts and daily-morning-digest
as subflows. The standalone deployments below allow them to also be triggered
independently (e.g. for testing or manual re-runs).

Usage:
  python flows/deploy_managed.py
  python flows/deploy_managed.py --work-pool my-pool
  python flows/deploy_managed.py --dry-run
"""



DEFAULT_WORK_POOL = "managed-pool"
GITHUB_REPO = "https://github.com/Octacon100/market-data-spark"
DEFAULT_BRANCH = "main"

PIP_PACKAGES = [
    "prefect-aws>=0.4.0",
    "boto3>=1.34.0",
    "botocore>=1.34.0",
    "pandas>=2.1.0",
    "pyarrow>=14.0.0",
    "numpy>=1.24.0",
    "requests>=2.31.0",
    "anthropic>=0.39.0",
    "python-dotenv>=1.0.0",
]


def deploy_all(work_pool: str, branch: str, dry_run: bool = False):


    source = GitRepository(url=GITHUB_REPO, branch=branch)

    deployments = [
        {
            "flow": reddit_scanner_flow,
            "entrypoint": "flows/reddit_scanner.py:reddit_scanner_flow",
            "name": "reddit-ticker-scanner",
            "schedule": CronSchedule(cron="0 */6 * * *", timezone="America/New_York"),
            "description": "Scan Reddit for trending stock ticker mentions every 6 hours",
            "tags": ["production", "reddit", "scanner"],
            "parameters": {},
        },
        {
            "flow": market_data_pipeline_with_glue,
            "entrypoint": "flows/market_data_with_glue.py:market_data_pipeline_with_glue",
            "name": "market-data-pipeline-glue",
            "schedule": CronSchedule(cron="0 18 * * *", timezone="America/New_York"),  # 6 PM ET daily
            "description": "Daily market data collection + Glue Spark analytics + buy signals + digest",
            "tags": ["production", "market-data", "glue"],
            "parameters": {},
        },
        {
            "flow": buy_signal_alert_flow,
            "entrypoint": "flows/buy_signal_alerts.py:buy_signal_alert_flow",
            "name": "buy-signal-alerts",
            "schedule": CronSchedule(cron="30 18 * * *", timezone="America/New_York"),  # 6:30 PM ET daily
            "description": "Standalone buy signal detection and email alerts",
            "tags": ["production", "signals", "alerts"],
            "parameters": {},
        },
        {
            "flow": daily_digest_flow,
            "entrypoint": "flows/daily_digest.py:daily_digest_flow",
            "name": "daily-morning-digest",
            "schedule": CronSchedule(cron="0 7 * * 1-5", timezone="America/New_York"),  # 7 AM ET, Mon-Fri
            "description": "Weekday morning email digest: buy signals, Reddit trends, price movers",
            "tags": ["production", "digest", "email"],
            "parameters": {},
        },
    ]

    print(f"\n[INFO] Repo   : {GITHUB_REPO} (branch: {branch})")
    print(f"[INFO] Pool   : {work_pool}")
    print(f"[INFO] Deployments: {len(deployments)}")
    if dry_run:
        print("[INFO] Dry run -- nothing will be deployed\n")

    for d in deployments:
        print(f"\n  {d['name']}")
        print(f"    Entrypoint : {d['entrypoint']}")
        print(f"    Schedule   : {d['schedule'].cron} ({d['schedule'].timezone})")
        print(f"    Tags       : {', '.join(d['tags'])}")

        if dry_run:
            continue

        try:
            d["flow"].from_source(
                source=source,
                entrypoint=d["entrypoint"],
            ).deploy(
                name=d["name"],
                work_pool_name=work_pool,
                schedule=d["schedule"],
                description=d["description"],
                tags=d["tags"],
                parameters=d["parameters"],
                job_variables={"pip_packages": PIP_PACKAGES},
            )
            print(f"    [OK] Deployed")
        except Exception as e:
            print(f"    [ERROR] {e}")

    if not dry_run:
        print("\n[OK] Done.")
        print("\nTo run manually:")
        print("  prefect deployment run 'reddit-ticker-scanner/reddit-ticker-scanner'")
        print("  prefect deployment run 'market-data-pipeline-with-glue/market-data-pipeline-glue'")
        print("  prefect deployment run 'buy-signal-alerts/buy-signal-alerts'")
        print("  prefect deployment run 'daily-morning-digest/daily-morning-digest'")
        print("\nView at: https://app.prefect.cloud")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy market data flows to Prefect Cloud")
    parser.add_argument(
        "--work-pool",
        default=DEFAULT_WORK_POOL,
        help=f"Prefect work pool name (default: {DEFAULT_WORK_POOL})",
    )
    parser.add_argument(
        "--branch",
        default=DEFAULT_BRANCH,
        help=f"Git branch to deploy from (default: {DEFAULT_BRANCH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deployments without creating them",
    )
    args = parser.parse_args()

    deploy_all(work_pool=args.work_pool, branch=args.branch, dry_run=args.dry_run)
