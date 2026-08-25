"""
Microsoft Fabric Spark Tasks for Market Data Pipeline

Triggers pre-created Fabric notebooks via the Fabric REST API (no SDK needed).

One-time setup:
  1. Create a Lakehouse in Fabric named 'MarketDataLakehouse'
  2. Upload spark_jobs/fabric/*.py notebooks to Fabric (or create them manually)
  3. Copy each notebook's item ID from its Fabric portal URL
  4. Set env vars (or Prefect Variables):
       FABRIC_WORKSPACE_ID            - your workspace GUID
       FABRIC_NOTEBOOK_DAILY_ANALYTICS - notebook item GUID
       FABRIC_NOTEBOOK_ML_FEATURES     - notebook item GUID
       FABRIC_NOTEBOOK_VOLATILITY      - notebook item GUID

Notebook item IDs appear in the Fabric portal URL:
  https://app.fabric.microsoft.com/groups/<workspace-id>/synapsenotebooks/<notebook-id>
"""

import time
import requests
from datetime import datetime
from prefect import task, flow
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
from config_utils import resolve
from notify import on_flow_complete, on_flow_failure


FABRIC_API = "https://api.fabric.microsoft.com/v1"


def _get_fabric_token() -> str:
    """Obtain an Azure access token for the Fabric REST API via client credentials."""
    tenant_id = resolve('azure-tenant-id', 'AZURE_TENANT_ID')
    client_id = resolve('azure-client-id', 'AZURE_CLIENT_ID')
    client_secret = resolve('azure-client-secret', 'AZURE_CLIENT_SECRET', is_secret=True)

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "Azure credentials not configured. Set AZURE_TENANT_ID, "
            "AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in .env or Prefect."
        )

    resp = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


@task(log_prints=True, tags=["fabric", "execute"])
def run_fabric_notebook(notebook_name: str, notebook_id: str) -> str:
    """
    Trigger a Fabric notebook run and return the job instance ID.

    Args:
        notebook_name: Human-readable label for logging
        notebook_id:   Fabric item GUID for the notebook

    Returns:
        str: Job instance ID (used to poll status)
    """
    workspace_id = resolve('fabric-workspace-id', 'FABRIC_WORKSPACE_ID')
    if not workspace_id:
        raise ValueError("FABRIC_WORKSPACE_ID not configured")

    token = _get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = (
        f"{FABRIC_API}/workspaces/{workspace_id}"
        f"/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    )

    print(f"[RUN] Starting Fabric notebook: {notebook_name}")
    resp = requests.post(url, headers=headers, json={}, timeout=30)

    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"Failed to start notebook '{notebook_name}': "
            f"{resp.status_code} {resp.text[:500]}"
        )

    location = resp.headers.get("Location", "")
    job_instance_id = location.rstrip("/").split("/")[-1]
    print(f"  [OK] Job instance: {job_instance_id}")

    emit_event(
        event="fabric.notebook.started",
        resource={
            "prefect.resource.id": f"fabric.notebook.{notebook_id}",
            "prefect.resource.name": notebook_name,
        },
        payload={"notebook_name": notebook_name, "job_instance_id": job_instance_id},
    )

    return job_instance_id


@task(log_prints=True, tags=["fabric", "monitor"])
def wait_for_fabric_notebook(
    notebook_name: str,
    notebook_id: str,
    job_instance_id: str,
    timeout_minutes: int = 30,
) -> dict:
    """
    Poll Fabric REST API until the notebook run finishes.

    Returns:
        dict: {"status": "success", "duration_minutes": float}
    """
    workspace_id = resolve('fabric-workspace-id', 'FABRIC_WORKSPACE_ID')
    token = _get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = (
        f"{FABRIC_API}/workspaces/{workspace_id}"
        f"/items/{notebook_id}/jobs/instances/{job_instance_id}"
    )

    start = time.time()
    last_token_refresh = start
    timeout = timeout_minutes * 60

    print(f"[WAIT] Polling: {notebook_name} (instance {job_instance_id})")

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise TimeoutError(
                f"Notebook '{notebook_name}' exceeded {timeout_minutes} min timeout"
            )

        # Refresh token every 45 minutes for long-running notebooks
        if time.time() - last_token_refresh > 2700:
            token = _get_fabric_token()
            headers = {"Authorization": f"Bearer {token}"}
            last_token_refresh = time.time()

        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")

        if status == "Completed":
            duration = elapsed / 60
            print(f"  [OK] {notebook_name} completed in {duration:.1f} min")
            emit_event(
                event="fabric.notebook.succeeded",
                resource={"prefect.resource.id": f"fabric.notebook.{notebook_id}"},
                payload={"notebook_name": notebook_name, "duration_minutes": duration},
            )
            return {"status": "success", "duration_minutes": duration}

        elif status in ("Failed", "Cancelled", "Deduped"):
            reason = data.get("failureReason", {})
            msg = reason.get("message", "Unknown error") if isinstance(reason, dict) else str(reason)
            emit_event(
                event="fabric.notebook.failed",
                resource={"prefect.resource.id": f"fabric.notebook.{notebook_id}"},
                payload={"notebook_name": notebook_name, "error": msg},
            )
            raise RuntimeError(f"Notebook '{notebook_name}' {status}: {msg}")

        else:
            print(f"  [WAIT] {notebook_name}: {status} ({elapsed/60:.1f} min elapsed)")
            time.sleep(30)


@flow(
    name="market-data-fabric-analytics",
    description="Run Spark analytics on Microsoft Fabric (notebooks via REST API)",
    log_prints=True,
    on_completion=[on_flow_complete],
    on_failure=[on_flow_failure],
)
def fabric_analytics_flow():
    """
    Trigger all three Fabric Spark notebooks and wait for each to complete.

    Notebook IDs are resolved from Prefect Variables or env vars:
      FABRIC_NOTEBOOK_DAILY_ANALYTICS
      FABRIC_NOTEBOOK_ML_FEATURES
      FABRIC_NOTEBOOK_VOLATILITY
    """
    start_time = datetime.now()
    workspace_id = resolve('fabric-workspace-id', 'FABRIC_WORKSPACE_ID')

    print(f"\n{'='*70}")
    print("[START] Microsoft Fabric Spark Analytics")
    print(f"{'='*70}")
    print(f"Started   : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace : {workspace_id}")
    print(f"{'='*70}\n")

    notebooks = [
        {
            "name": "daily-analytics",
            "prefect_var": "fabric-notebook-daily-analytics",
            "env_var": "FABRIC_NOTEBOOK_DAILY_ANALYTICS",
        },
        {
            "name": "ml-features",
            "prefect_var": "fabric-notebook-ml-features",
            "env_var": "FABRIC_NOTEBOOK_ML_FEATURES",
        },
        {
            "name": "volatility-metrics",
            "prefect_var": "fabric-notebook-volatility",
            "env_var": "FABRIC_NOTEBOOK_VOLATILITY",
        },
    ]

    job_results = []

    for nb in notebooks:
        notebook_id = resolve(nb["prefect_var"], nb["env_var"])
        if not notebook_id:
            print(f"  [WARN] {nb['name']}: notebook ID not configured ({nb['env_var']}), skipping")
            job_results.append({"name": nb["name"], "status": "skipped"})
            continue

        try:
            job_instance_id = run_fabric_notebook(nb["name"], notebook_id)
            result = wait_for_fabric_notebook(nb["name"], notebook_id, job_instance_id)
            job_results.append({
                "name": nb["name"],
                "status": "success",
                "duration_minutes": result["duration_minutes"],
            })
        except Exception as e:
            print(f"[ERROR] {nb['name']} failed: {e}")
            job_results.append({"name": nb["name"], "status": "failed", "error": str(e)})

    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds() / 60
    success_count = len([r for r in job_results if r["status"] == "success"])
    compute_minutes = sum(
        r.get("duration_minutes", 0) for r in job_results if r["status"] == "success"
    )

    # Fabric Spark cost approximation: F4 SKU at ~$0.36/CU-hour, ~4 CUs per notebook
    estimated_cost = (compute_minutes / 60) * 0.36 * 4

    summary_md = f"""# Fabric Spark Analytics Summary

## Execution
- **Started:** {start_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Completed:** {end_time.strftime('%Y-%m-%d %H:%M:%S')}
- **Total Duration:** {total_duration:.1f} min

## Results
| Notebook | Status | Duration |
|----------|--------|----------|
"""
    for r in job_results:
        icon = "[OK]" if r["status"] == "success" else ("[SKIP]" if r["status"] == "skipped" else "[ERROR]")
        dur = f"{r['duration_minutes']:.1f} min" if r.get("duration_minutes") else r.get("error", "-")
        summary_md += f"| {r['name']} | {icon} {r['status']} | {dur} |\n"

    summary_md += f"""
## Cost Estimate (F4 SKU approximation)
- **Compute Time:** {compute_minutes:.1f} min
- **Estimated Cost:** ${estimated_cost:.2f}

## Output (OneLake)
- `Files/analytics/daily_stats/`
- `Files/analytics/ml_features/`
- `Files/analytics/volatility_metrics/`

## Fabric Portal
https://app.fabric.microsoft.com/groups/{workspace_id}
"""

    create_markdown_artifact(
        key=f"fabric-analytics-{start_time.strftime('%Y%m%d%H%M%S')}",
        markdown=summary_md,
        description="Fabric Spark analytics summary",
    )

    print(f"\n{'='*70}")
    print("[COMPLETE] Fabric Analytics Done")
    print(f"  Duration: {total_duration:.1f} min  |  Success: {success_count}/{len(job_results)}")
    print(f"  Estimated Cost: ${estimated_cost:.2f}")
    print(f"{'='*70}\n")

    return {
        "job_results": job_results,
        "total_duration_minutes": total_duration,
        "estimated_cost": estimated_cost,
    }


if __name__ == "__main__":
    fabric_analytics_flow()
