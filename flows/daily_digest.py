"""
Daily Morning Digest Email - Prefect Flow
Sends a single consolidated HTML email at 7 AM ET on weekdays combining:
  - Active buy signals from ML features
  - Top Reddit trending tickers
  - Watchlist price movers (biggest % changes)
  - New tickers added to watchlist from Reddit
"""

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
import boto3
import pandas as pd
import smtplib
from email.message import EmailMessage
import io
import os
import json
from datetime import datetime, date
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================

SETTINGS_PATH = Path(__file__).parent.parent / "config" / "pipeline_settings.json"
WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "watchlist.json"


def load_settings():
    """Load pipeline settings from config/pipeline_settings.json"""
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, "r") as f:
            return json.load(f)
    return {}


def load_watchlist():
    """Load stock symbols from config/watchlist.json"""
    if WATCHLIST_PATH.exists():
        with open(WATCHLIST_PATH, "r") as f:
            return json.load(f)
    return []


# ============================================================================
# Data Gathering Tasks
# ============================================================================

@task(log_prints=True, tags=["digest", "signals"])
def gather_buy_signals(bucket: str) -> list:
    """
    Read ML features from S3 and return active buy signals.
    Reuses same detection logic as buy_signal_alerts.py.

    Args:
        bucket: S3 bucket name

    Returns:
        list of signal dicts with symbol, signal, description, price, strength
    """
    settings = load_settings()
    rules = settings.get("buy_signals", {}).get("rules", {})

    if not settings.get("buy_signals", {}).get("enabled", True):
        print("[INFO] Buy signals disabled in pipeline_settings.json")
        return []

    s3 = boto3.client("s3")
    prefix = "analytics/ml_features/"

    try:
        paginator = s3.get_paginator("list_objects_v2")
        parquet_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_keys.append(obj["Key"])

        if not parquet_keys:
            print("[WARN] No ML features data found - run Spark analytics first")
            return []

        frames = []
        for key in parquet_keys:
            resp = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(resp["Body"].read())
            frames.append(pd.read_parquet(buf))

        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        latest = df.sort_values("timestamp").groupby("symbol").tail(2)

    except Exception as e:
        print(f"[ERROR] Failed to read ML features: {e}")
        return []

    signals = []

    for symbol in latest["symbol"].unique():
        sym_data = latest[latest["symbol"] == symbol].sort_values("timestamp")
        if len(sym_data) < 2:
            continue
        prev = sym_data.iloc[-2]
        curr = sym_data.iloc[-1]

        # Golden Cross
        if rules.get("golden_cross", {}).get("enabled", False):
            if prev.get("ma_7d", 0) <= prev.get("ma_30d", 0) and curr.get("ma_7d", 0) > curr.get("ma_30d", 0):
                signals.append({
                    "symbol": symbol, "signal": "Golden Cross",
                    "description": "7d MA crossed above 30d MA",
                    "price": float(curr.get("price", 0)), "strength": "strong"
                })

        # Momentum Surge
        min_mom = rules.get("momentum_surge", {}).get("min_momentum_pct", 3.0)
        if rules.get("momentum_surge", {}).get("enabled", False):
            momentum = curr.get("price_momentum_7d", 0)
            if momentum and momentum >= min_mom:
                signals.append({
                    "symbol": symbol, "signal": "Momentum Surge",
                    "description": f"7d momentum: {momentum:.1f}% (min {min_mom}%)",
                    "price": float(curr.get("price", 0)), "strength": "strong" if momentum >= min_mom * 2 else "moderate"
                })

        # Oversold Bounce
        if rules.get("oversold_bounce", {}).get("enabled", False):
            if prev.get("price", 0) < prev.get("ma_30d", 0) and curr.get("price", 0) > curr.get("ma_30d", 0):
                signals.append({
                    "symbol": symbol, "signal": "Oversold Bounce",
                    "description": "Price recovered above 30d MA",
                    "price": float(curr.get("price", 0)), "strength": "moderate"
                })

        # Low Vol Uptrend
        max_vol = rules.get("low_vol_uptrend", {}).get("max_volatility_7d_pct", 2.0)
        min_mom2 = rules.get("low_vol_uptrend", {}).get("min_momentum_pct", 1.0)
        if rules.get("low_vol_uptrend", {}).get("enabled", False):
            vol = curr.get("volatility_7d")
            momentum = curr.get("price_momentum_7d", 0)
            if vol is not None and vol < max_vol and momentum and momentum >= min_mom2:
                signals.append({
                    "symbol": symbol, "signal": "Low Vol Uptrend",
                    "description": f"Stable uptrend (vol: {vol:.2f}, mom: {momentum:.1f}%)",
                    "price": float(curr.get("price", 0)), "strength": "moderate"
                })

    print(f"[OK] Buy signals: {len(signals)}")
    return signals


@task(log_prints=True, tags=["digest", "reddit"])
def gather_reddit_trending(bucket: str, top_n: int = 10) -> list:
    """
    Read today's Reddit trending tickers from S3.

    Args:
        bucket: S3 bucket name
        top_n: How many top tickers to include

    Returns:
        list of dicts with symbol, mentions, subreddits
    """
    s3 = boto3.client("s3")
    today = date.today().strftime("%Y-%m-%d")
    key = f"reddit/trending/{today}.json"

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read())
        trending = data if isinstance(data, list) else data.get("trending", [])
        trending_sorted = sorted(trending, key=lambda x: x.get("mentions", 0), reverse=True)
        result = trending_sorted[:top_n]
        print(f"[OK] Reddit trending: {len(result)} tickers loaded")
        return result

    except s3.exceptions.NoSuchKey:
        print(f"[WARN] No Reddit data for today ({today}) - run reddit_scanner_flow first")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to read Reddit trending: {e}")
        return []


@task(log_prints=True, tags=["digest", "movers"])
def gather_price_movers(bucket: str, top_n: int = 10) -> list:
    """
    Read daily analytics from S3 and return biggest price movers.

    Args:
        bucket: S3 bucket name
        top_n: Number of top movers to return

    Returns:
        list of dicts with symbol, price, price_change_pct, volume, sorted by abs(change)
    """
    s3 = boto3.client("s3")
    prefix = "analytics/daily_stats/"

    try:
        paginator = s3.get_paginator("list_objects_v2")
        parquet_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_keys.append(obj["Key"])

        if not parquet_keys:
            print("[WARN] No daily stats data found in S3")
            return []

        frames = []
        for key in parquet_keys:
            resp = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(resp["Body"].read())
            frames.append(pd.read_parquet(buf))

        df = pd.concat(frames, ignore_index=True)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            latest = df.sort_values("timestamp").groupby("symbol").tail(1)
        else:
            latest = df.groupby("symbol").tail(1)

        # Sort by absolute price change
        change_col = None
        for col in ["price_change_pct", "change_pct", "pct_change"]:
            if col in latest.columns:
                change_col = col
                break

        if change_col is None:
            print("[WARN] No price change column found in daily stats")
            return []

        latest = latest.dropna(subset=[change_col])
        latest["_abs_change"] = latest[change_col].abs()
        top_movers = latest.nlargest(top_n, "_abs_change")

        result = []
        for _, row in top_movers.iterrows():
            mover = {
                "symbol": row.get("symbol", ""),
                "price": float(row.get("price", row.get("close", 0))),
                "change_pct": float(row[change_col]),
            }
            if "volume" in row:
                mover["volume"] = int(row["volume"])
            result.append(mover)

        print(f"[OK] Price movers: {len(result)} tickers loaded")
        return result

    except Exception as e:
        print(f"[ERROR] Failed to read daily stats: {e}")
        return []


@task(log_prints=True, tags=["digest", "watchlist"])
def gather_new_watchlist_additions(bucket: str) -> list:
    """
    Detect tickers added to watchlist today by comparing S3 history
    against the current watchlist.json.

    Falls back to returning an empty list if no history is available.

    Args:
        bucket: S3 bucket name

    Returns:
        list of newly added ticker symbols
    """
    s3 = boto3.client("s3")
    today = date.today().strftime("%Y-%m-%d")
    key = f"watchlist/history/{today}.json"

    current_watchlist = set(load_watchlist())

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        today_data = json.loads(resp["Body"].read())
        new_additions = today_data.get("added", [])
        print(f"[OK] New watchlist additions today: {len(new_additions)}")
        return new_additions

    except Exception:
        # No history file - cannot determine new additions
        print("[INFO] No watchlist history for today - skipping new additions section")
        return []


# ============================================================================
# Email Composition and Sending
# ============================================================================

def _signal_row_html(sig: dict) -> str:
    """Render a single buy signal table row as HTML."""
    color = "#2e7d32" if sig["strength"] == "strong" else "#f57f17"
    return (
        f'<tr>'
        f'<td style="padding:6px 10px;border:1px solid #ddd;font-weight:bold">{sig["symbol"]}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">{sig["signal"]}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">{sig["description"]}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">${sig["price"]:.2f}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd;color:{color}">{sig["strength"]}</td>'
        f'</tr>'
    )


def _mover_row_html(mover: dict) -> str:
    """Render a price mover table row as HTML."""
    change = mover["change_pct"]
    color = "#2e7d32" if change >= 0 else "#c62828"
    arrow = "+" if change >= 0 else ""
    volume_cell = f'{mover["volume"]:,}' if "volume" in mover else "-"
    return (
        f'<tr>'
        f'<td style="padding:6px 10px;border:1px solid #ddd;font-weight:bold">{mover["symbol"]}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">${mover["price"]:.2f}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd;color:{color}">{arrow}{change:.2f}%</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">{volume_cell}</td>'
        f'</tr>'
    )


def _reddit_row_html(ticker: dict) -> str:
    """Render a Reddit trending row as HTML."""
    symbol = ticker.get("symbol", ticker.get("ticker", ""))
    mentions = ticker.get("mentions", 0)
    subreddits = ", ".join(ticker.get("subreddits", []))
    return (
        f'<tr>'
        f'<td style="padding:6px 10px;border:1px solid #ddd;font-weight:bold">{symbol}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">{mentions}</td>'
        f'<td style="padding:6px 10px;border:1px solid #ddd">{subreddits}</td>'
        f'</tr>'
    )


def _section_header_html(title: str) -> str:
    return f'<h3 style="color:#1565c0;border-bottom:2px solid #1565c0;padding-bottom:4px;margin-top:28px">{title}</h3>'


def _table_html(headers: list, rows_html: str) -> str:
    header_cells = "".join(
        f'<th style="padding:6px 10px;background:#1565c0;color:white;border:1px solid #ddd;text-align:left">{h}</th>'
        for h in headers
    )
    return (
        f'<table style="border-collapse:collapse;width:100%;margin-bottom:12px">'
        f'<tr>{header_cells}</tr>'
        f'{rows_html}'
        f'</table>'
    )


def _no_data_html(msg: str) -> str:
    return f'<p style="color:#888;font-style:italic">{msg}</p>'


@task(log_prints=True, tags=["digest", "email"])
def compose_and_send_digest(
    signals: list,
    trending: list,
    movers: list,
    new_additions: list,
):
    """
    Compose HTML digest email and send via Gmail SMTP.

    Requires env vars:
    - ALERT_EMAIL_FROM: Gmail sender address
    - ALERT_EMAIL_TO: Recipient address
    - GMAIL_APP_PASSWORD: Google App Password

    Args:
        signals: Buy signal list from gather_buy_signals
        trending: Reddit trending list from gather_reddit_trending
        movers: Price movers list from gather_price_movers
        new_additions: New watchlist symbols added today
    """
    settings = load_settings()
    alert_config = settings.get("alerts", {})

    if not alert_config.get("email_enabled", False):
        print("[INFO] Email disabled in pipeline_settings.json")
        return

    email_from = os.getenv("ALERT_EMAIL_FROM", "")
    email_to = os.getenv("ALERT_EMAIL_TO", "")
    app_password = os.getenv("GMAIL_APP_PASSWORD", "")

    if not all([email_from, email_to, app_password]):
        print("[WARN] Gmail not configured - need ALERT_EMAIL_FROM, ALERT_EMAIL_TO, GMAIL_APP_PASSWORD")
        return

    today_str = date.today().strftime("%A, %B %d %Y")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"[Market Data] Morning Digest - {today_str}"

    # ------------------------------------------------------------------ HTML
    # Buy Signals section
    if signals:
        signal_rows = "".join(_signal_row_html(s) for s in signals)
        signals_html = _table_html(["Symbol", "Signal", "Detail", "Price", "Strength"], signal_rows)
    else:
        signals_html = _no_data_html("No buy signals detected today.")

    # Price Movers section
    if movers:
        mover_rows = "".join(_mover_row_html(m) for m in movers)
        movers_html = _table_html(["Symbol", "Price", "Change", "Volume"], mover_rows)
    else:
        movers_html = _no_data_html("No price mover data available.")

    # Reddit Trending section
    if trending:
        reddit_rows = "".join(_reddit_row_html(t) for t in trending)
        reddit_html = _table_html(["Symbol", "Mentions", "Subreddits"], reddit_rows)
    else:
        reddit_html = _no_data_html("No Reddit trending data available for today.")

    # New Watchlist Additions section
    if new_additions:
        additions_list = "".join(
            f'<li style="margin:4px 0"><strong>{sym}</strong> - added from Reddit scan</li>'
            for sym in new_additions
        )
        additions_html = f'<ul style="padding-left:20px">{additions_list}</ul>'
    else:
        additions_html = _no_data_html("No new tickers added to watchlist today.")

    # Signal badge for subject line
    signal_badge = (
        f'<span style="background:#2e7d32;color:white;padding:2px 8px;border-radius:4px;font-size:13px">'
        f'{len(signals)} signal(s)'
        f'</span>'
        if signals else
        '<span style="background:#888;color:white;padding:2px 8px;border-radius:4px;font-size:13px">no signals</span>'
    )

    html_body = f"""
    <html>
    <body style="font-family:Arial,sans-serif;max-width:760px;margin:0 auto;color:#222">
        <div style="background:#1565c0;padding:18px 24px;border-radius:6px 6px 0 0">
            <h1 style="color:white;margin:0;font-size:22px">Market Data - Morning Digest</h1>
            <p style="color:#bbdefb;margin:4px 0 0">{today_str} &nbsp;|&nbsp; Generated: {now_str} &nbsp;|&nbsp; {signal_badge}</p>
        </div>
        <div style="padding:20px 24px;border:1px solid #ddd;border-top:none;border-radius:0 0 6px 6px">

            {_section_header_html("Buy Signals")}
            {signals_html}

            {_section_header_html("Watchlist Price Movers (Top 10)")}
            {movers_html}

            {_section_header_html("Reddit Trending Tickers (Top 10)")}
            {reddit_html}

            {_section_header_html("New Watchlist Additions from Reddit")}
            {additions_html}

            <p style="color:#aaa;font-size:11px;margin-top:32px;border-top:1px solid #eee;padding-top:12px">
                Market Data Pipeline &mdash; For informational purposes only. Not financial advice.
            </p>
        </div>
    </body>
    </html>"""

    # ----------------------------------------------------------------- plain text
    text_lines = [
        f"Market Data - Morning Digest",
        f"Date: {today_str}",
        f"Generated: {now_str}",
        "",
        "== BUY SIGNALS ==",
    ]
    if signals:
        for s in signals:
            text_lines.append(f"  {s['symbol']} | {s['signal']} | {s['description']} | ${s['price']:.2f} | {s['strength']}")
    else:
        text_lines.append("  No buy signals today.")

    text_lines += ["", "== PRICE MOVERS =="]
    if movers:
        for m in movers:
            arrow = "+" if m["change_pct"] >= 0 else ""
            text_lines.append(f"  {m['symbol']} | ${m['price']:.2f} | {arrow}{m['change_pct']:.2f}%")
    else:
        text_lines.append("  No data available.")

    text_lines += ["", "== REDDIT TRENDING =="]
    if trending:
        for t in trending:
            sym = t.get("symbol", t.get("ticker", ""))
            text_lines.append(f"  {sym} | {t.get('mentions', 0)} mentions")
    else:
        text_lines.append("  No data available.")

    text_lines += ["", "== NEW WATCHLIST ADDITIONS =="]
    if new_additions:
        for sym in new_additions:
            text_lines.append(f"  {sym}")
    else:
        text_lines.append("  None today.")

    text_lines += ["", "---", "Not financial advice. Do your own research."]
    text_body = "\n".join(text_lines)

    # ----------------------------------------------------------------- send
    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_from, app_password)
            server.send_message(msg)
        print(f"[OK] Digest email sent to {email_to}")
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] Gmail auth failed - check GMAIL_APP_PASSWORD")
    except Exception as e:
        print(f"[ERROR] Failed to send digest: {e}")


@task(log_prints=True, tags=["digest", "artifacts"])
def create_digest_artifact(signals: list, trending: list, movers: list, new_additions: list):
    """Create a Prefect markdown artifact summarizing the digest content."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    today_str = date.today().strftime("%Y-%m-%d")

    signal_rows = ""
    for s in signals:
        signal_rows += f"| {s['symbol']} | {s['signal']} | {s['description']} | ${s['price']:.2f} | {s['strength']} |\n"

    mover_rows = ""
    for m in movers:
        arrow = "+" if m["change_pct"] >= 0 else ""
        mover_rows += f"| {m['symbol']} | ${m['price']:.2f} | {arrow}{m['change_pct']:.2f}% |\n"

    reddit_rows = ""
    for t in trending:
        sym = t.get("symbol", t.get("ticker", ""))
        reddit_rows += f"| {sym} | {t.get('mentions', 0)} |\n"

    additions_list = ", ".join(new_additions) if new_additions else "None"

    markdown = f"""# Daily Digest - {today_str}
**Generated:** {now_str}

## Buy Signals ({len(signals)})

| Symbol | Signal | Detail | Price | Strength |
|--------|--------|--------|-------|----------|
{signal_rows if signal_rows else "| - | No signals | - | - | - |\n"}

## Price Movers (Top {len(movers)})

| Symbol | Price | Change |
|--------|-------|--------|
{mover_rows if mover_rows else "| - | No data | - |\n"}

## Reddit Trending (Top {len(trending)})

| Symbol | Mentions |
|--------|----------|
{reddit_rows if reddit_rows else "| - | No data |\n"}

## New Watchlist Additions
{additions_list}

> Not financial advice.
"""

    create_markdown_artifact(
        key=f"daily-digest-{today_str}",
        markdown=markdown,
        description=f"Morning digest for {today_str}",
    )
    print("[OK] Digest artifact created")


# ============================================================================
# Daily Digest Flow
# ============================================================================

@flow(
    name="daily-morning-digest",
    description="Send a consolidated morning email with buy signals, Reddit trends, and price movers",
    log_prints=True,
)
def daily_digest_flow(bucket: str = None, top_n_reddit: int = 10, top_n_movers: int = 10):
    """
    Morning digest flow - runs at 7 AM ET on weekdays.

    Steps:
    1. Read buy signals from ML features (S3 parquet)
    2. Read Reddit trending tickers (S3 JSON, today)
    3. Read biggest price movers from daily stats (S3 parquet)
    4. Check for new watchlist additions added by reddit_scanner_flow
    5. Create Prefect artifact with summary
    6. Send consolidated HTML email via Gmail SMTP

    Args:
        bucket: S3 bucket name (defaults to S3_BUCKET env var)
        top_n_reddit: How many Reddit trending tickers to include
        top_n_movers: How many price movers to include
    """
    if bucket is None:
        bucket = os.getenv("S3_BUCKET", "")
        if not bucket:
            print("[ERROR] S3_BUCKET not set")
            return {"error": "S3_BUCKET not set"}

    print("\n" + "=" * 60)
    print("[DIGEST] Daily Morning Digest")
    print("=" * 60)

    # Gather all data (tasks run in parallel where Prefect allows)
    signals = gather_buy_signals(bucket)
    trending = gather_reddit_trending(bucket, top_n=top_n_reddit)
    movers = gather_price_movers(bucket, top_n=top_n_movers)
    new_additions = gather_new_watchlist_additions(bucket)

    # Create artifact (always - even if email fails)
    create_digest_artifact(signals, trending, movers, new_additions)

    # Send email
    compose_and_send_digest(signals, trending, movers, new_additions)

    print(f"\n[COMPLETE] Digest finished - signals:{len(signals)} "
          f"trending:{len(trending)} movers:{len(movers)} additions:{len(new_additions)}")

    return {
        "signal_count": len(signals),
        "trending_count": len(trending),
        "mover_count": len(movers),
        "new_additions": new_additions,
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    daily_digest_flow()
