"""
Deploy market data flows (Microsoft Fabric edition) to a self-hosted Prefect server.

This mirrors deploy_local.py but deploys the Fabric pipeline instead of the
Glue pipeline. Both can coexist in the same Prefect work pool.

Deployments:
  social-ticker-scanner         - Every 6 hours (same as AWS version)
  market-data-pipeline-fabric   - Daily at 7 AM ET
  daily-morning-digest          - Weekdays at 7:30 AM ET

Setup:
  1. Configure OneLake credentials in .env (see .env.example)
  2. Create Fabric Lakehouse + notebooks (see spark_jobs/fabric/README.md)
  3. Set FABRIC_NOTEBOOK_* env vars with notebook item IDs
  4. Run: docker-compose exec worker python flows/deploy_fabric.py
"""

import argparse
from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository

from market_data_with_fabric import market_data_pipeline_with_fabric
from daily_digest import daily_digest_flow
from reddit_scanner import reddit_scanner_flow


DEFAULT_WORK_POOL = "default-agent-pool"
GITHUB_REPO = "https://github.com/Octacon100/market-data-spark"
DEFAULT_BRANCH = "main"


def deploy_all(work_pool: str, branch: str, dry_run: bool = False):

    source = GitRepository(url=GITHUB_REPO, branch=branch)

    deployments = [
        {
            "flow": reddit_scanner_flow,
            "entrypoint": "flows/reddit_scanner.py:reddit_scanner_flow",
            "name": "social-ticker-scanner",
            "schedule": CronSchedule(cron="0 */6 * * *", timezone="America/New_York"),
            "description": "Scan Reddit, StockTwits, and Yahoo Finance for trending tickers",
            "tags": ["production", "social", "scanner", "fabric"],
            "parameters": {},
        },
        {
            "flow": market_data_pipeline_with_fabric,
            "entrypoint": "flows/market_data_with_fabric.py:market_data_pipeline_with_fabric",
            "name": "market-data-pipeline-fabric",
            "schedule": CronSchedule(cron="0 7 * * *", timezone="America/New_York"),
            "description": "Daily market data + OneLake storage + Fabric Spark analytics",
            "tags": ["production", "market-data", "fabric", "onelake"],
            "parameters": {},
        },
        {
            "flow": daily_digest_flow,
            "entrypoint": "flows/daily_digest.py:daily_digest_flow",
            "name": "daily-morning-digest-fabric",
            "schedule": CronSchedule(cron="30 7 * * 1-5", timezone="America/New_York"),
            "description": "Weekday morning email digest (Fabric edition)",
            "tags": ["production", "digest", "email", "fabric"],
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
            )
            print(f"    [OK] Deployed")
        except Exception as e:
            print(f"    [ERROR] {e}")

    if not dry_run:
        print("\n[OK] Done.")
        print("\nTo run manually:")
        print("  prefect deployment run 'social-ticker-scanner/social-ticker-scanner'")
        print("  prefect deployment run 'market-data-pipeline-fabric/market-data-pipeline-fabric'")
        print("  prefect deployment run 'daily-morning-digest/daily-morning-digest-fabric'")
        print("\nView at: http://localhost:4200")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deploy Fabric edition flows to local Prefect server"
    )
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
