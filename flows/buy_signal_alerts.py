"""
Buy Signal Detection and Email Alerts
Reads ML features from S3, evaluates buy signals, and sends email via AWS SES.
"""

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
import boto3
import pandas as pd
import io
import os
import json
from datetime import datetime
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================

SETTINGS_PATH = Path(__file__).parent.parent / "config" / "pipeline_settings.json"


def load_settings():
    """Load pipeline settings from config/pipeline_settings.json"""
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, "r") as f:
            return json.load(f)
    return {}


# ============================================================================
# Signal Detection
# ============================================================================

@task(log_prints=True, tags=["signals", "analytics"])
def detect_buy_signals(bucket: str) -> list:
    """
    Read ML features from S3 and detect buy signals.

    Signals detected:
    - Golden Cross: 7-day MA crosses above 30-day MA
    - Momentum Surge: Strong positive 7-day momentum
    - Oversold Bounce: Price recovers above 30-day MA
    - Low Vol Uptrend: Low volatility with positive momentum

    Args:
        bucket: S3 bucket name

    Returns:
        list: Detected buy signals with details
    """
    settings = load_settings()
    rules = settings.get("buy_signals", {}).get("rules", {})

    if not settings.get("buy_signals", {}).get("enabled", False):
        print("[INFO] Buy signal detection is disabled in pipeline_settings.json")
        return []

    s3 = boto3.client("s3")

    # Read ML features from S3
    input_prefix = "analytics/ml_features/"
    print(f"[INFO] Reading ML features from s3://{bucket}/{input_prefix}")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        parquet_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=input_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_keys.append(obj["Key"])

        if not parquet_keys:
            print("[WARN] No ML features data found in S3 - run Spark analytics first")
            return []

        # Read all parquet files into a single DataFrame
        frames = []
        for key in parquet_keys:
            response = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(response["Body"].read())
            frames.append(pd.read_parquet(buf))

        df = pd.concat(frames, ignore_index=True)
        print(f"[OK] Loaded {len(df)} records from ML features")

    except Exception as e:
        print(f"[ERROR] Failed to read ML features: {e}")
        return []

    if df.empty:
        print("[WARN] ML features DataFrame is empty")
        return []

    # Get the most recent data per symbol
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    latest = df.sort_values("timestamp").groupby("symbol").tail(2)

    signals = []

    for symbol in latest["symbol"].unique():
        sym_data = latest[latest["symbol"] == symbol].sort_values("timestamp")

        if len(sym_data) < 2:
            continue

        prev = sym_data.iloc[-2]
        curr = sym_data.iloc[-1]

        # Signal 1: Golden Cross (7d MA crosses above 30d MA)
        if rules.get("golden_cross", {}).get("enabled", False):
            prev_cross = prev.get("ma_7d", 0) <= prev.get("ma_30d", 0)
            curr_cross = curr.get("ma_7d", 0) > curr.get("ma_30d", 0)
            if prev_cross and curr_cross:
                signals.append({
                    "symbol": symbol,
                    "signal": "Golden Cross",
                    "description": "7-day MA crossed above 30-day MA",
                    "price": float(curr.get("price", 0)),
                    "ma_7d": float(curr.get("ma_7d", 0)),
                    "ma_30d": float(curr.get("ma_30d", 0)),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "strong"
                })

        # Signal 2: Momentum Surge
        min_momentum = rules.get("momentum_surge", {}).get("min_momentum_pct", 3.0)
        if rules.get("momentum_surge", {}).get("enabled", False):
            momentum = curr.get("price_momentum_7d", 0)
            if momentum and momentum >= min_momentum:
                signals.append({
                    "symbol": symbol,
                    "signal": "Momentum Surge",
                    "description": f"7-day momentum at {momentum:.1f}% (threshold: {min_momentum}%)",
                    "price": float(curr.get("price", 0)),
                    "momentum_7d": float(momentum),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "strong" if momentum >= min_momentum * 2 else "moderate"
                })

        # Signal 3: Oversold Bounce
        if rules.get("oversold_bounce", {}).get("enabled", False):
            prev_below = prev.get("price", 0) < prev.get("ma_30d", 0)
            curr_above = curr.get("price", 0) > curr.get("ma_30d", 0)
            if prev_below and curr_above:
                signals.append({
                    "symbol": symbol,
                    "signal": "Oversold Bounce",
                    "description": "Price recovered above 30-day MA",
                    "price": float(curr.get("price", 0)),
                    "ma_30d": float(curr.get("ma_30d", 0)),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "moderate"
                })

        # Signal 4: Low Volatility Uptrend
        max_vol = rules.get("low_vol_uptrend", {}).get("max_volatility_7d_pct", 2.0)
        min_mom = rules.get("low_vol_uptrend", {}).get("min_momentum_pct", 1.0)
        if rules.get("low_vol_uptrend", {}).get("enabled", False):
            vol_7d = curr.get("volatility_7d", None)
            momentum = curr.get("price_momentum_7d", 0)
            if vol_7d is not None and vol_7d < max_vol and momentum and momentum >= min_mom:
                signals.append({
                    "symbol": symbol,
                    "signal": "Low Vol Uptrend",
                    "description": f"Stable uptrend (vol: {vol_7d:.2f}, momentum: {momentum:.1f}%)",
                    "price": float(curr.get("price", 0)),
                    "volatility_7d": float(vol_7d),
                    "momentum_7d": float(momentum),
                    "timestamp": str(curr["timestamp"]),
                    "strength": "moderate"
                })

    if signals:
        print(f"[OK] Detected {len(signals)} buy signal(s):")
        for sig in signals:
            print(f"  - {sig['symbol']}: {sig['signal']} ({sig['description']})")
    else:
        print("[INFO] No buy signals detected")

    return signals


# ============================================================================
# Email Alerts
# ============================================================================

@task(log_prints=True, tags=["alerts", "email"])
def send_email_alerts(signals: list):
    """
    Send buy signal alerts via AWS SES.

    Requires:
    - ALERT_EMAIL_SENDER in .env (verified SES sender)
    - recipients configured in config/pipeline_settings.json
    - SES sender/recipients verified in AWS console

    Args:
        signals: List of detected buy signals
    """
    if not signals:
        print("[INFO] No signals to send")
        return

    settings = load_settings()
    alert_config = settings.get("alerts", {})

    if not alert_config.get("email_enabled", False):
        print("[INFO] Email alerts disabled in pipeline_settings.json - set alerts.email_enabled to true")
        return

    recipients = alert_config.get("recipients", [])
    if not recipients:
        print("[WARN] No email recipients configured in pipeline_settings.json")
        return

    sender = os.getenv("ALERT_EMAIL_SENDER", "")
    if not sender:
        print("[WARN] ALERT_EMAIL_SENDER not set in .env - skipping email")
        return

    region = os.getenv("AWS_REGION", "us-east-1")
    ses = boto3.client("ses", region_name=region)

    # Build email body
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[Market Data] {len(signals)} Buy Signal(s) Detected - {now}"

    # Plain text body
    text_lines = [
        "Market Data Pipeline - Buy Signal Alert",
        f"Generated: {now}",
        f"Signals Detected: {len(signals)}",
        "",
        "=" * 50,
    ]

    for sig in signals:
        text_lines.extend([
            "",
            f"  Symbol: {sig['symbol']}",
            f"  Signal: {sig['signal']}",
            f"  Detail: {sig['description']}",
            f"  Price:  ${sig['price']:.2f}",
            f"  Strength: {sig['strength']}",
            f"  Time:  {sig['timestamp']}",
            "-" * 50,
        ])

    text_lines.extend([
        "",
        "NOTE: These signals are for informational purposes only.",
        "They are not financial advice. Always do your own research.",
    ])

    text_body = "\n".join(text_lines)

    # HTML body
    signal_rows = ""
    for sig in signals:
        strength_color = "#2e7d32" if sig["strength"] == "strong" else "#f57f17"
        signal_rows += f"""
        <tr>
            <td style="padding:8px;border:1px solid #ddd;font-weight:bold">{sig['symbol']}</td>
            <td style="padding:8px;border:1px solid #ddd">{sig['signal']}</td>
            <td style="padding:8px;border:1px solid #ddd">{sig['description']}</td>
            <td style="padding:8px;border:1px solid #ddd">${sig['price']:.2f}</td>
            <td style="padding:8px;border:1px solid #ddd;color:{strength_color}">{sig['strength']}</td>
        </tr>"""

    html_body = f"""
    <html>
    <body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto">
        <h2 style="color:#1565c0">Market Data Pipeline - Buy Signal Alert</h2>
        <p>Generated: {now} | Signals: {len(signals)}</p>
        <table style="border-collapse:collapse;width:100%">
            <tr style="background:#1565c0;color:white">
                <th style="padding:8px;border:1px solid #ddd">Symbol</th>
                <th style="padding:8px;border:1px solid #ddd">Signal</th>
                <th style="padding:8px;border:1px solid #ddd">Detail</th>
                <th style="padding:8px;border:1px solid #ddd">Price</th>
                <th style="padding:8px;border:1px solid #ddd">Strength</th>
            </tr>
            {signal_rows}
        </table>
        <p style="color:#888;font-size:12px;margin-top:20px">
            These signals are for informational purposes only.
            They are not financial advice. Always do your own research.
        </p>
    </body>
    </html>"""

    try:
        ses.send_email(
            Source=sender,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        print(f"[OK] Email sent to {', '.join(recipients)}")

    except ses.exceptions.MessageRejected as e:
        print(f"[ERROR] SES rejected the message: {e}")
        print("[INFO] Verify your sender and recipients in the AWS SES console")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")
        print("[INFO] Check AWS SES setup - sender and recipients must be verified")


# ============================================================================
# Prefect Artifact (always created, even without email)
# ============================================================================

@task(log_prints=True, tags=["signals", "artifacts"])
def create_signal_artifact(signals: list):
    """Create a Prefect markdown artifact summarizing detected signals."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not signals:
        summary = f"""# Buy Signal Report
**Generated:** {now}

No buy signals detected for current watchlist.
"""
    else:
        rows = ""
        for sig in signals:
            rows += f"| {sig['symbol']} | {sig['signal']} | {sig['description']} | ${sig['price']:.2f} | {sig['strength']} |\n"

        summary = f"""# Buy Signal Report
**Generated:** {now}
**Signals Detected:** {len(signals)}

| Symbol | Signal | Detail | Price | Strength |
|--------|--------|--------|-------|----------|
{rows}
> These signals are for informational purposes only.
"""

    create_markdown_artifact(
        key=f"buy-signals-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        markdown=summary,
        description="Buy signal detection results",
    )

    # Emit event for Prefect Cloud
    if signals:
        emit_event(
            event="market.buy_signals.detected",
            resource={
                "prefect.resource.id": "market-data-buy-signals",
                "prefect.resource.name": "Buy Signal Detection",
            },
            payload={
                "signal_count": len(signals),
                "symbols": list(set(s["symbol"] for s in signals)),
                "signals": [
                    {"symbol": s["symbol"], "signal": s["signal"], "strength": s["strength"]}
                    for s in signals
                ],
            },
        )

    print(f"[OK] Signal artifact created")


# ============================================================================
# Buy Signal Alert Flow
# ============================================================================

@flow(
    name="buy-signal-alerts",
    description="Detect buy signals from ML features and send email alerts",
    log_prints=True,
)
def buy_signal_alert_flow(bucket: str = None):
    """
    Detect buy signals and send alerts.

    Steps:
    1. Read ML features from S3
    2. Evaluate buy signal rules from pipeline_settings.json
    3. Create Prefect artifact with results
    4. Send email via AWS SES (if configured)

    Args:
        bucket: S3 bucket name (defaults to S3_BUCKET env var)

    Returns:
        dict: Signal detection results
    """
    if bucket is None:
        bucket = os.getenv("S3_BUCKET", "")
        if not bucket:
            print("[ERROR] S3_BUCKET not set")
            return {"signals": [], "error": "S3_BUCKET not set"}

    print("\n" + "=" * 60)
    print("[SIGNALS] Buy Signal Detection")
    print("=" * 60)

    # Step 1: Detect signals
    signals = detect_buy_signals(bucket)

    # Step 2: Create artifact (always)
    create_signal_artifact(signals)

    # Step 3: Send email (if configured and signals found)
    if signals:
        send_email_alerts(signals)

    print(f"\n[COMPLETE] Signal detection finished: {len(signals)} signal(s)")

    return {"signals": signals, "count": len(signals)}


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    buy_signal_alert_flow()
