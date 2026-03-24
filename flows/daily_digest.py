"""
Daily Digest Flow - Claude AI Stock Synopsis
Reads Reddit trending tickers, generates Claude AI analysis for each stock,
and sends a daily digest email at 7 AM.
"""

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
import boto3
import json
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path

import anthropic
import dotenv

dotenv.load_dotenv()

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
# Reddit Trending Data
# ============================================================================

@task(log_prints=True, tags=["digest", "reddit"])
def load_reddit_trending(bucket: str) -> dict:
    """
    Load the latest Reddit trending ticker data from S3.

    Reads from reddit/trending/{date}.json stored by the reddit scanner flow.

    Args:
        bucket: S3 bucket name

    Returns:
        dict: Trending data with ticker mention counts, or empty dict
    """
    s3 = boto3.client("s3")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    s3_key = f"reddit/trending/{date_str}.json"

    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        ticker_count = data.get("total_unique_tickers", 0)
        print(f"[OK] Loaded Reddit trending data: {ticker_count} tickers from {s3_key}")
        return data
    except s3.exceptions.NoSuchKey:
        print(f"[WARN] No Reddit trending data for today ({s3_key}) - using watchlist only")
        return {}
    except Exception as e:
        print(f"[WARN] Could not load Reddit trending data: {e}")
        return {}


# ============================================================================
# Stock Data Gathering
# ============================================================================

@task(log_prints=True, tags=["digest", "data"])
def gather_stock_context(tickers: list, bucket: str) -> dict:
    """
    Gather available analytics data for each ticker from S3.

    Reads ML features, daily stats, and volatility metrics produced
    by the Spark analytics jobs.

    Args:
        tickers: List of ticker symbols to gather data for
        bucket: S3 bucket name

    Returns:
        dict: Mapping of ticker -> context data dict
    """
    import pandas as pd
    import io

    s3 = boto3.client("s3")
    stock_data = {}

    # Read ML features
    ml_features = _read_parquet_prefix(s3, bucket, "analytics/ml_features/")
    daily_stats = _read_parquet_prefix(s3, bucket, "analytics/daily_stats/")
    volatility = _read_parquet_prefix(s3, bucket, "analytics/volatility_metrics/")

    for ticker in tickers:
        context = {"symbol": ticker}

        # Extract latest ML features for this ticker
        if ml_features is not None and not ml_features.empty:
            ticker_ml = ml_features[ml_features["symbol"] == ticker]
            if not ticker_ml.empty:
                latest = ticker_ml.sort_values("timestamp").iloc[-1]
                context["price"] = float(latest.get("price", 0))
                context["ma_7d"] = float(latest.get("ma_7d", 0))
                context["ma_30d"] = float(latest.get("ma_30d", 0))
                context["momentum_7d"] = float(latest.get("price_momentum_7d", 0))
                context["volatility_7d"] = float(latest.get("volatility_7d", 0))
                context["volatility_30d"] = float(latest.get("volatility_30d", 0))

        # Extract daily stats
        if daily_stats is not None and not daily_stats.empty:
            ticker_daily = daily_stats[daily_stats["symbol"] == ticker]
            if not ticker_daily.empty:
                latest = ticker_daily.sort_values("date").iloc[-1]
                context["avg_price"] = float(latest.get("avg_price", 0))
                context["daily_volume"] = float(latest.get("total_volume", 0))
                context["daily_range_pct"] = float(latest.get("range_pct", 0))

        # Extract volatility metrics
        if volatility is not None and not volatility.empty:
            ticker_vol = volatility[volatility["symbol"] == ticker]
            if not ticker_vol.empty:
                latest = ticker_vol.sort_values("date").iloc[-1]
                context["annualized_volatility"] = float(latest.get("annualized_volatility", 0))
                context["large_move_count"] = int(latest.get("large_move_days", 0))

        stock_data[ticker] = context

    print(f"[OK] Gathered context data for {len(stock_data)} tickers")
    return stock_data


def _read_parquet_prefix(s3, bucket, prefix):
    """Read all parquet files under an S3 prefix into a single DataFrame."""
    import pandas as pd
    import io

    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])

        if not keys:
            return None

        frames = []
        for key in keys:
            response = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(response["Body"].read())
            frames.append(pd.read_parquet(buf))

        return pd.concat(frames, ignore_index=True)
    except Exception:
        return None


# ============================================================================
# Claude AI Synopsis Generation
# ============================================================================

@task(log_prints=True, tags=["digest", "claude", "ai"], retries=2, retry_delay_seconds=30)
def generate_claude_synopsis(ticker: str, context: dict, reddit_mentions: int) -> dict:
    """
    Generate a Claude AI synopsis for a single stock ticker.

    Asks Claude to evaluate investment viability based on available data
    and the metrics requested in issue #11.

    Args:
        ticker: Stock symbol
        context: Available analytics data for the ticker
        reddit_mentions: Number of Reddit mentions

    Returns:
        dict: Synopsis with ticker, analysis text, and metadata
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(f"[WARN] ANTHROPIC_API_KEY not set - skipping Claude synopsis for {ticker}")
        return {
            "symbol": ticker,
            "synopsis": f"Claude synopsis unavailable (API key not configured) for {ticker}.",
            "error": "no_api_key",
        }

    settings = load_settings()
    digest_config = settings.get("daily_digest", {})
    model = digest_config.get("claude_model", "claude-sonnet-4-20250514")
    max_tokens = digest_config.get("max_tokens_per_stock", 500)

    # Build context string from available data
    data_lines = [f"Ticker: {ticker}", f"Reddit mentions (24h): {reddit_mentions}"]

    if "price" in context:
        data_lines.append(f"Current price: ${context['price']:.2f}")
    if "ma_7d" in context:
        data_lines.append(f"7-day moving average: ${context['ma_7d']:.2f}")
    if "ma_30d" in context:
        data_lines.append(f"30-day moving average: ${context['ma_30d']:.2f}")
    if "momentum_7d" in context:
        data_lines.append(f"7-day price momentum: {context['momentum_7d']:.1f}%")
    if "volatility_7d" in context:
        data_lines.append(f"7-day volatility: {context['volatility_7d']:.2f}")
    if "volatility_30d" in context:
        data_lines.append(f"30-day volatility: {context['volatility_30d']:.2f}")
    if "annualized_volatility" in context:
        data_lines.append(f"Annualized volatility: {context['annualized_volatility']:.2f}")
    if "daily_volume" in context:
        data_lines.append(f"Daily volume: {context['daily_volume']:,.0f}")
    if "daily_range_pct" in context:
        data_lines.append(f"Daily range: {context['daily_range_pct']:.2f}%")
    if "large_move_count" in context:
        data_lines.append(f"Large move days (>1%): {context['large_move_count']}")

    data_block = "\n".join(data_lines)

    prompt = f"""You are a stock analyst providing a concise daily digest synopsis. Given the
data below for {ticker}, provide a brief investment assessment covering as many of the following
as your knowledge allows:

- P/E ratio and earnings per share (EPS)
- Dividend information (upcoming amounts, yield)
- Recent news events that could impact the stock price
- Reddit community sentiment (based on mention count provided)
- Sector rotation indicators
- Earnings surprise rate and revision trends
- 6-12 month historical return context
- Return on invested capital (ROIC)
- Earnings stability and accrual levels
- EV/EBITDA valuation
- Long-term performance patterns

Available pipeline data:
{data_block}

Write 3-5 sentences. Be direct and factual. If specific fundamental data is not available from
the pipeline, use your general knowledge of the stock. Flag any data you are uncertain about.
End with a one-word verdict: BULLISH, BEARISH, or NEUTRAL."""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        synopsis_text = response.content[0].text.strip()
        print(f"[OK] Claude synopsis generated for {ticker} ({len(synopsis_text)} chars)")

        return {
            "symbol": ticker,
            "synopsis": synopsis_text,
            "reddit_mentions": reddit_mentions,
            "model": model,
        }

    except Exception as e:
        print(f"[ERROR] Claude API call failed for {ticker}: {e}")
        return {
            "symbol": ticker,
            "synopsis": f"Synopsis generation failed for {ticker}: {e}",
            "error": str(e),
        }


# ============================================================================
# Digest Email
# ============================================================================

@task(log_prints=True, tags=["digest", "email"])
def send_digest_email(synopses: list, signal_results: dict = None):
    """
    Send the daily digest email with Claude synopses via Gmail SMTP.

    Args:
        synopses: List of synopsis dicts from generate_claude_synopsis
        signal_results: Optional buy signal results to include
    """
    if not synopses:
        print("[INFO] No synopses to send")
        return

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

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_display = datetime.now().strftime("%B %d, %Y")
    subject = f"[Market Data] Daily Digest - {date_display}"

    # Plain text body
    text_lines = [
        "Market Data Pipeline - Daily Stock Digest",
        f"Generated: {now}",
        f"Stocks Analyzed: {len(synopses)}",
        "",
        "=" * 60,
    ]

    for s in synopses:
        text_lines.extend([
            "",
            f"--- {s['symbol']} (Reddit mentions: {s.get('reddit_mentions', 'N/A')}) ---",
            "",
            s["synopsis"],
            "",
        ])

    if signal_results and signal_results.get("signals"):
        text_lines.extend(["", "=" * 60, "BUY SIGNALS DETECTED:", ""])
        for sig in signal_results["signals"]:
            text_lines.append(
                f"  {sig['symbol']}: {sig['signal']} - {sig['description']} "
                f"(${sig['price']:.2f}, {sig['strength']})"
            )

    text_lines.extend([
        "",
        "=" * 60,
        "NOTE: This digest is AI-generated and for informational purposes only.",
        "It is not financial advice. Always do your own research.",
    ])

    text_body = "\n".join(text_lines)

    # HTML body
    synopsis_html = ""
    for s in synopses:
        mentions_badge = ""
        mentions = s.get("reddit_mentions", 0)
        if mentions and mentions > 0:
            mentions_badge = (
                f' <span style="background:#ff4500;color:white;padding:2px 8px;'
                f'border-radius:12px;font-size:11px">'
                f'Reddit: {mentions} mentions</span>'
            )

        # Color the verdict if present
        synopsis_text = s["synopsis"]
        for verdict, color in [("BULLISH", "#2e7d32"), ("BEARISH", "#c62828"), ("NEUTRAL", "#f57f17")]:
            synopsis_text = synopsis_text.replace(
                verdict,
                f'<span style="font-weight:bold;color:{color}">{verdict}</span>',
            )

        error_note = ""
        if s.get("error"):
            error_note = (
                '<p style="color:#c62828;font-size:12px">'
                f'[Note: {s["error"]}]</p>'
            )

        synopsis_html += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:16px;margin:12px 0;
                     background:#fafafa">
            <h3 style="margin:0 0 8px 0;color:#1565c0">{s['symbol']}{mentions_badge}</h3>
            <p style="margin:0;line-height:1.6;color:#333">{synopsis_text}</p>
            {error_note}
        </div>"""

    signals_html = ""
    if signal_results and signal_results.get("signals"):
        signal_rows = ""
        for sig in signal_results["signals"]:
            strength_color = "#2e7d32" if sig["strength"] == "strong" else "#f57f17"
            signal_rows += f"""
            <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;font-weight:bold">{sig['symbol']}</td>
                <td style="padding:6px 10px;border:1px solid #ddd">{sig['signal']}</td>
                <td style="padding:6px 10px;border:1px solid #ddd">{sig['description']}</td>
                <td style="padding:6px 10px;border:1px solid #ddd">${sig['price']:.2f}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;color:{strength_color}">{sig['strength']}</td>
            </tr>"""

        signals_html = f"""
        <h2 style="color:#1565c0;margin-top:24px">Buy Signals</h2>
        <table style="border-collapse:collapse;width:100%">
            <tr style="background:#1565c0;color:white">
                <th style="padding:8px;border:1px solid #ddd">Symbol</th>
                <th style="padding:8px;border:1px solid #ddd">Signal</th>
                <th style="padding:8px;border:1px solid #ddd">Detail</th>
                <th style="padding:8px;border:1px solid #ddd">Price</th>
                <th style="padding:8px;border:1px solid #ddd">Strength</th>
            </tr>
            {signal_rows}
        </table>"""

    html_body = f"""
    <html>
    <body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px">
        <h1 style="color:#1565c0;border-bottom:2px solid #1565c0;padding-bottom:8px">
            Daily Stock Digest
        </h1>
        <p style="color:#666">Generated: {now} | Stocks: {len(synopses)}</p>

        <h2 style="color:#1565c0">AI Stock Analysis</h2>
        {synopsis_html}

        {signals_html}

        <p style="color:#888;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">
            This digest is AI-generated using Claude by Anthropic.
            It is for informational purposes only and is not financial advice.
            Always do your own research before making investment decisions.
        </p>
    </body>
    </html>"""

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
        print(f"[OK] Daily digest email sent to {email_to}")
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] Gmail authentication failed - check GMAIL_APP_PASSWORD")
    except Exception as e:
        print(f"[ERROR] Failed to send digest email: {e}")


# ============================================================================
# Prefect Artifact
# ============================================================================

@task(log_prints=True, tags=["digest", "artifacts"])
def create_digest_artifact(synopses: list):
    """Create a Prefect markdown artifact with the daily digest."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not synopses:
        summary = f"""# Daily Stock Digest
**Generated:** {now}

No stock synopses generated.
"""
    else:
        stock_sections = ""
        for s in synopses:
            mentions = s.get("reddit_mentions", "N/A")
            stock_sections += f"""
### {s['symbol']} (Reddit: {mentions} mentions)

{s['synopsis']}

---
"""

        summary = f"""# Daily Stock Digest
**Generated:** {now}
**Stocks Analyzed:** {len(synopses)}

{stock_sections}

> This digest is AI-generated and for informational purposes only.
"""

    create_markdown_artifact(
        key=f"daily-digest-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        markdown=summary,
        description="Daily stock digest with Claude AI analysis",
    )

    print("[OK] Digest artifact created")


# ============================================================================
# Daily Digest Flow
# ============================================================================

@flow(
    name="daily-digest",
    description="Generate Claude AI synopsis for Reddit-trending stocks and send daily email",
    log_prints=True,
)
def daily_digest_flow(bucket: str = None, signal_results: dict = None):
    """
    Daily digest flow - Claude AI stock analysis.

    Steps:
    1. Load Reddit trending tickers from S3
    2. Merge with watchlist for full coverage
    3. Gather available analytics data per ticker
    4. Generate Claude AI synopsis for each Reddit-trending stock
    5. Create Prefect artifact
    6. Send digest email

    Args:
        bucket: S3 bucket name (defaults to S3_BUCKET env var)
        signal_results: Optional buy signal results to include in email

    Returns:
        dict: Digest results
    """
    if bucket is None:
        bucket = os.getenv("S3_BUCKET", "")
        if not bucket:
            print("[ERROR] S3_BUCKET not set")
            return {"synopses": [], "error": "S3_BUCKET not set"}

    settings = load_settings()
    digest_config = settings.get("daily_digest", {})

    if not digest_config.get("enabled", True):
        print("[INFO] Daily digest is disabled in pipeline_settings.json")
        return {"synopses": [], "disabled": True}

    max_stocks = digest_config.get("max_stocks", 10)

    print("\n" + "=" * 60)
    print("[DIGEST] Daily Stock Digest with Claude AI")
    print("=" * 60)

    # Step 1: Load Reddit trending
    trending = load_reddit_trending(bucket)
    reddit_tickers = trending.get("tickers", {})

    # Step 2: Build ticker list - Reddit trending stocks are the focus
    # Fall back to watchlist if no Reddit data
    if reddit_tickers:
        # Sort by mention count, take top N
        sorted_tickers = sorted(reddit_tickers.items(), key=lambda x: x[1], reverse=True)
        target_tickers = [t for t, _ in sorted_tickers[:max_stocks]]
        print(f"[INFO] Analyzing top {len(target_tickers)} Reddit-trending tickers")
    else:
        watchlist = load_watchlist()
        target_tickers = watchlist[:max_stocks]
        print(f"[INFO] No Reddit data - analyzing {len(target_tickers)} watchlist tickers")

    if not target_tickers:
        print("[WARN] No tickers to analyze")
        return {"synopses": [], "warning": "no_tickers"}

    # Step 3: Gather analytics data
    stock_data = gather_stock_context(target_tickers, bucket)

    # Step 4: Generate Claude synopsis for each ticker
    synopses = []
    for ticker in target_tickers:
        mentions = reddit_tickers.get(ticker, 0)
        context = stock_data.get(ticker, {"symbol": ticker})
        synopsis = generate_claude_synopsis(ticker, context, mentions)
        synopses.append(synopsis)

    successful = [s for s in synopses if not s.get("error")]
    print(f"[OK] Generated {len(successful)}/{len(synopses)} synopses successfully")

    # Step 5: Create artifact
    create_digest_artifact(synopses)

    # Step 6: Send email
    send_digest_email(synopses, signal_results=signal_results)

    # Emit event
    emit_event(
        event="market.daily_digest.completed",
        resource={
            "prefect.resource.id": "daily-stock-digest",
            "prefect.resource.name": "Daily Stock Digest",
        },
        payload={
            "stocks_analyzed": len(synopses),
            "successful_synopses": len(successful),
            "tickers": target_tickers,
            "source": "reddit" if reddit_tickers else "watchlist",
        },
    )

    print(f"\n[COMPLETE] Daily digest finished: {len(synopses)} stocks analyzed")

    return {
        "synopses": synopses,
        "stocks_analyzed": len(synopses),
        "successful": len(successful),
        "source": "reddit" if reddit_tickers else "watchlist",
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    daily_digest_flow()
