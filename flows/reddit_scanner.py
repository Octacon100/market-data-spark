"""
Reddit Ticker Scanner - Prefect Flow
Scans Reddit communities for stock ticker mentions and stores trending data to S3.
"""

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
from notify import on_flow_complete, on_flow_failure
import boto3
import requests
import re
import time
import json
import os
from datetime import datetime, timedelta, timezone
from collections import Counter
from pathlib import Path

import dotenv
from config_utils import resolve, make_boto3_client

dotenv.load_dotenv()

# ============================================================================
# Configuration
# ============================================================================

from config_utils import SETTINGS_PATH, WATCHLIST_PATH, IGNORE_LIST_PATH


def load_ignore_list():
    """Load the Reddit ignore list from config/reddit_ignore_list.json"""
    if IGNORE_LIST_PATH.exists():
        with open(IGNORE_LIST_PATH, "r") as f:
            return set(json.load(f))
    return set()


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


def save_watchlist(watchlist):
    """Save stock symbols to config/watchlist.json"""
    with open(WATCHLIST_PATH, "w") as f:
        json.dump(watchlist, f, indent=4)


def save_ignore_list(ignore_list):
    """Save ignore list to config/reddit_ignore_list.json"""
    with open(IGNORE_LIST_PATH, "w") as f:
        json.dump(list(ignore_list), f, indent=4)


# Common false positives - words that look like tickers but aren't
FALSE_POSITIVES = {
    "I", "A", "AM", "PM", "DD", "CEO", "CFO", "CTO", "COO",
    "IPO", "ETF", "GDP", "EPS", "PE", "FDA", "SEC", "FED",
    "IT", "IS", "AT", "ON", "OR", "AN", "AS", "IF", "DO",
    "SO", "NO", "UP", "GO", "TO", "IN", "BE", "BY", "HE",
    "ME", "MY", "US", "WE", "OK", "OP", "TD", "RH", "UI",
    "TV", "AI", "ML", "PC", "UK", "EU", "UN", "DC", "LA",
    "NY", "SF", "TX", "CA", "FL", "OH", "MA", "PA", "VA",
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL",
    "CAN", "HER", "WAS", "ONE", "OUR", "OUT", "HAS", "HIS",
    "HOW", "MAN", "NEW", "NOW", "OLD", "SEE", "WAY", "WHO",
    "DID", "GET", "HIM", "LET", "SAY", "SHE", "TOO", "USE",
    "DAD", "MOM", "BRO", "LOL", "IMO", "TBH", "SMH", "FYI",
    "YOLO", "FOMO", "HODL", "MOON", "BEAR", "BULL", "LONG",
    "SHORT", "CALL", "PUTS", "PUMP", "DUMP", "SELL", "HOLD",
    "EDIT", "TLDR", "LMAO", "ROFL", "INFO", "NEWS", "POST",
    "JUST", "LIKE", "GOOD", "BEST", "NEXT", "MOST", "VERY",
    "EVER", "MUCH", "ALSO", "BACK", "BEEN", "COME", "EACH",
    "EVEN", "SOME", "THAN", "THEM", "THEN", "ONLY", "WELL",
    "ALSO", "MANY", "REAL", "HUGE", "GAIN", "LOSS", "HIGH",
    "DEEP", "OPEN", "CASH", "FREE", "SAFE", "RISK", "DEBT",
    "FUND", "TECH", "RATE", "BOND", "BANK", "LOAN", "SAVE",
    "GOLD", "COST", "FAIR", "PAYS", "YEAR", "WEEK",
    "RIP", "ETA", "AMA", "PSA", "TIL",
}

# Regex pattern to match ticker mentions
# Matches $AAPL or standalone uppercase 1-5 letter words
TICKER_PATTERN_DOLLAR = re.compile(r'\$([A-Z]{1,5})\b')
TICKER_PATTERN_BARE = re.compile(r'\b([A-Z]{1,5})\b')


# ============================================================================
# Tasks
# ============================================================================

@task(log_prints=True, tags=["reddit", "api"])
def create_reddit_client():
    """
    Create a requests session for Reddit's OAuth API.
    Requires REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET env vars.
    Falls back to public JSON API if credentials are missing.

    Returns:
        dict with 'session' (requests.Session) and 'base_url' (str)
    """
    user_agent = os.getenv("REDDIT_USER_AGENT", "market-data-scanner/1.0 (by /u/market-data-bot)")
    client_id = resolve("reddit-client-id", "REDDIT_CLIENT_ID")
    client_secret = resolve("reddit-client-secret", "REDDIT_CLIENT_SECRET", is_secret=True)

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    if client_id and client_secret:
        auth_resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": user_agent},
            timeout=10,
        )
        auth_resp.raise_for_status()
        token = auth_resp.json()["access_token"]
        session.headers.update({"Authorization": f"Bearer {token}"})
        print("[OK] Reddit OAuth client created (oauth.reddit.com)")
        return {"session": session, "base_url": "https://oauth.reddit.com"}
    else:
        print("[WARN] No Reddit credentials found - using public JSON API (may be rate-limited)")
        print("[INFO] Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET for reliable access")
        print("[INFO] Create a free app at https://www.reddit.com/prefs/apps (type: script)")
        return {"session": session, "base_url": "https://www.reddit.com"}


@task(log_prints=True, tags=["reddit", "scan"])
def scan_subreddit(client, subreddit_name, lookback_hours=24, post_limit=100):
    """
    Scan a subreddit for ticker mentions using Reddit's API.

    Args:
        client: dict with 'session' (requests.Session) and 'base_url' (str)
        subreddit_name: Name of subreddit to scan (without r/)
        lookback_hours: How far back to look for posts
        post_limit: Maximum number of posts to scan (max 100 per request)

    Returns:
        Counter: Ticker mention counts from this subreddit
    """
    session = client["session"]
    base_url = client["base_url"]

    print(f"[INFO] Scanning r/{subreddit_name} (last {lookback_hours}h)...")
    mentions = Counter()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    ignore_set = load_ignore_list()

    if ignore_set:
        print(f"  [INFO] Ignoring {len(ignore_set)} tickers from ignore list")

    posts_scanned = 0

    try:
        url = f"{base_url}/r/{subreddit_name}/new.json"
        resp = session.get(url, params={"limit": min(post_limit, 100)}, timeout=10)
        resp.raise_for_status()
        posts = resp.json()["data"]["children"]

        for post_wrapper in posts:
            post = post_wrapper["data"]
            post_time = datetime.fromtimestamp(post["created_utc"], timezone.utc)
            if post_time < cutoff:
                continue

            posts_scanned += 1
            text = f"{post.get('title', '')} {post.get('selftext', '')}"
            tickers = extract_tickers(text, ignore_set=ignore_set)
            for ticker in tickers:
                mentions[ticker] += 1

            time.sleep(1.0 if base_url.endswith("reddit.com") else 0.5)
            comments_url = f"{base_url}/r/{subreddit_name}/comments/{post['id']}.json"
            try:
                cresp = session.get(comments_url, params={"limit": 20, "depth": 1}, timeout=10)
                cresp.raise_for_status()
                comment_listing = cresp.json()
                if len(comment_listing) > 1:
                    for c in comment_listing[1]["data"]["children"][:20]:
                        body = c["data"].get("body", "")
                        for ticker in extract_tickers(body, ignore_set=ignore_set):
                            mentions[ticker] += 1
            except Exception as ce:
                print(f"  [WARN] Could not fetch comments for post {post['id']}: {ce}")

        print(f"  [OK] r/{subreddit_name}: scanned {posts_scanned} posts, "
              f"found {len(mentions)} unique tickers")
        if ignore_set:
            print(f"  [INFO] Ignored tickers filtered: {', '.join(sorted(ignore_set))}")

    except Exception as e:
        print(f"  [ERROR] Failed to scan r/{subreddit_name}: {e}")

    return mentions


def extract_tickers(text, ignore_set=None):
    """
    Extract stock tickers from text.

    Matches:
    - $AAPL style (dollar sign prefix)
    - AAPL style (bare uppercase, 2-5 chars only for bare matches)

    Filters out common false positives and user-configured ignore list.

    Args:
        text: Raw text to scan
        ignore_set: Optional set of tickers to ignore (from reddit_ignore_list.json)

    Returns:
        list: Extracted ticker symbols
    """
    if ignore_set is None:
        ignore_set = set()

    skip = FALSE_POSITIVES | ignore_set
    tickers = []

    # Dollar-sign tickers (high confidence, allow 1 char like $F)
    dollar_matches = TICKER_PATTERN_DOLLAR.findall(text)
    for match in dollar_matches:
        upper = match.upper()
        if upper not in skip:
            tickers.append(upper)

    # Bare tickers (lower confidence, require 2-5 chars)
    bare_matches = TICKER_PATTERN_BARE.findall(text)
    for match in bare_matches:
        if len(match) < 2:
            continue
        upper = match.upper()
        if upper not in skip and upper not in tickers:
            tickers.append(upper)

    return tickers


@task(log_prints=True, tags=["reddit", "aggregate"])
def aggregate_mentions(subreddit_mentions, min_mentions=2):
    """
    Aggregate ticker mentions across all subreddits.

    Args:
        subreddit_mentions: List of (subreddit_name, Counter) tuples
        min_mentions: Minimum mentions to include in results

    Returns:
        dict: Aggregated results with per-ticker and per-subreddit breakdown
    """
    total = Counter()
    by_subreddit = {}

    for sub_name, mentions in subreddit_mentions:
        by_subreddit[sub_name] = dict(mentions.most_common(50))
        total += mentions

    # Filter by minimum mentions
    filtered = {
        ticker: count
        for ticker, count in total.most_common()
        if count >= min_mentions
    }

    results = {
        "scan_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_unique_tickers": len(filtered),
        "tickers": filtered,
        "by_subreddit": by_subreddit,
        "top_20": dict(Counter(filtered).most_common(20)),
    }

    print(f"[OK] Aggregated {len(filtered)} tickers above {min_mentions} mentions")
    if filtered:
        top5 = Counter(filtered).most_common(5)
        for ticker, count in top5:
            print(f"  {ticker}: {count} mentions")

    return results


@task(log_prints=True, tags=["reddit", "s3"])
def store_trending_to_s3(results, bucket):
    """
    Store trending ticker results to S3 as JSON.

    Storage path: reddit/trending/{date}.json

    Args:
        results: Aggregated trending data
        bucket: S3 bucket name

    Returns:
        str: S3 key where data was stored
    """
    s3 = make_boto3_client("s3")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"reddit/trending/{date_str}.json"

    json_data = json.dumps(results, indent=2, default=str)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json",
            Metadata={
                "source": "reddit-scanner",
                "scan_date": date_str,
                "ticker_count": str(results.get("total_unique_tickers", 0)),
            },
        )
        s3_uri = f"s3://{bucket}/{s3_key}"
        print(f"[OK] Stored trending data: {s3_uri}")
        return s3_key

    except Exception as e:
        print(f"[ERROR] Failed to store trending data to S3: {e}")
        raise


@task(log_prints=True, tags=["reddit", "watchlist"])
def update_watchlist_from_trending(results, auto_add_top_n=5, min_mentions=10):
    """
    Merge top trending tickers into config/watchlist.json.

    - Reads existing watchlist
    - Adds top N tickers that meet minimum mention threshold
    - Skips tickers already in watchlist
    - Marks new entries with source: reddit
    - Does NOT remove existing manual tickers

    Args:
        results: Aggregated trending data from scan
        auto_add_top_n: Number of top tickers to consider adding
        min_mentions: Minimum mentions required to add

    Returns:
        dict: Summary of what was added/skipped
    """
    settings = load_settings()
    reddit_config = settings.get("reddit_scanner", {})
    top_n = reddit_config.get("auto_add_top_n", auto_add_top_n)
    threshold = reddit_config.get("min_mentions", min_mentions)

    watchlist = load_watchlist()
    existing_set = set(watchlist)

    tickers = results.get("tickers", {})
    # Sort by mention count descending
    sorted_tickers = sorted(tickers.items(), key=lambda x: x[1], reverse=True)

    added = []
    skipped = []

    for ticker, count in sorted_tickers[:top_n]:
        if count < threshold:
            skipped.append({"ticker": ticker, "count": count, "reason": "below_threshold"})
            continue
        if ticker in existing_set:
            skipped.append({"ticker": ticker, "count": count, "reason": "already_in_watchlist"})
            continue

        watchlist.append(ticker)
        existing_set.add(ticker)
        added.append({"ticker": ticker, "count": count, "source": "reddit"})

    if added:
        save_watchlist(watchlist)
        # Write history to S3 so daily_digest can show new additions
        bucket = resolve('s3-bucket', 'S3_BUCKET')
        if bucket:
            try:
                s3 = make_boto3_client("s3")
                today = datetime.now(timezone.utc).date().isoformat()
                history = {"date": today, "added": [e["ticker"] for e in added]}
                s3.put_object(
                    Bucket=bucket,
                    Key=f"watchlist/history/{today}.json",
                    Body=json.dumps(history),
                    ContentType="application/json",
                )
                print(f"[OK] Watchlist history written to S3 for {today}")
            except Exception as e:
                print(f"[WARN] Could not write watchlist history to S3: {e}")
        print(f"[OK] Added {len(added)} ticker(s) to watchlist:")
        for entry in added:
            print(f"  + {entry['ticker']} ({entry['count']} mentions)")
    else:
        print("[INFO] No new tickers to add to watchlist")

    if skipped:
        for entry in skipped:
            print(f"  - Skipped {entry['ticker']}: {entry['reason']}")

    summary = {
        "added": added,
        "skipped": skipped,
        "watchlist_size": len(watchlist),
    }

    # Create Prefect artifact
    added_rows = ""
    for entry in added:
        added_rows += f"| {entry['ticker']} | {entry['count']} | reddit | NEW |\n"
    skipped_rows = ""
    for entry in skipped:
        skipped_rows += f"| {entry['ticker']} | {entry['count']} | - | {entry['reason']} |\n"

    no_added_row = "| - | - | - | No new tickers |\n"
    no_skipped_row = "| - | - | - | None skipped |\n"

    artifact_md = f"""# Watchlist Update Summary
**Timestamp:** {datetime.now(timezone.utc).isoformat()}

## Added ({len(added)})
| Ticker | Mentions | Source | Status |
|--------|----------|--------|--------|
{added_rows if added_rows else no_added_row}

## Skipped ({len(skipped)})
| Ticker | Mentions | Source | Reason |
|--------|----------|--------|--------|
{skipped_rows if skipped_rows else no_skipped_row}

**Watchlist Size:** {len(watchlist)} symbols
"""

    create_markdown_artifact(
        key=f"watchlist-update-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        markdown=artifact_md,
        description="Watchlist auto-update from Reddit trending",
    )

    return summary


# ============================================================================
# StockTwits Scanner
# ============================================================================

@task(log_prints=True, tags=["stocktwits", "scan"])
def scan_stocktwits_trending():
    """
    Fetch trending symbols from StockTwits public API (no auth required).

    Returns:
        Counter: Ticker mention counts based on watchlist_count
    """
    print("[INFO] Fetching StockTwits trending symbols...")
    mentions = Counter()
    ignore_set = load_ignore_list()
    skip = FALSE_POSITIVES | ignore_set

    try:
        resp = requests.get(
            "https://api.stocktwits.com/api/2/trending/symbols.json",
            headers={"User-Agent": "market-data-scanner/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        for symbol_data in data.get("symbols", []):
            ticker = symbol_data.get("symbol", "")
            if ticker and ticker not in skip:
                score = symbol_data.get("watchlist_count", 1)
                mentions[ticker] = score

        print(f"  [OK] StockTwits: {len(mentions)} trending symbols")

    except Exception as e:
        print(f"  [ERROR] StockTwits API failed: {e}")

    return mentions


# ============================================================================
# Yahoo Finance Trending Scanner
# ============================================================================

@task(log_prints=True, tags=["yahoo", "scan"])
def scan_yahoo_trending():
    """
    Fetch trending tickers from Yahoo Finance (no auth required).

    Returns:
        Counter: Ticker mention counts (each trending ticker gets score 1)
    """
    print("[INFO] Fetching Yahoo Finance trending tickers...")
    mentions = Counter()
    ignore_set = load_ignore_list()
    skip = FALSE_POSITIVES | ignore_set

    try:
        resp = requests.get(
            "https://query2.finance.yahoo.com/v1/finance/trending/US",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        quotes = data.get("finance", {}).get("result", [])
        if quotes:
            for item in quotes[0].get("quotes", []):
                ticker = item.get("symbol", "")
                if ticker and ticker.isalpha() and ticker not in skip:
                    mentions[ticker] = 1

        print(f"  [OK] Yahoo Finance: {len(mentions)} trending tickers")

    except Exception as e:
        print(f"  [WARN] Yahoo Finance trending failed: {e}")

    return mentions


# ============================================================================
# Flow
# ============================================================================

@flow(
    name="social-ticker-scanner",
    description="Scan Reddit, StockTwits, and Yahoo Finance for trending stock tickers",
    log_prints=True,
    on_completion=[on_flow_complete],
    on_failure=[on_flow_failure],
)
def reddit_scanner_flow(
    subreddits=None,
    lookback_hours=24,
    min_mentions=2,
    auto_update_watchlist=True,
    bucket=None,
):
    """
    Scan multiple social/financial sources for stock ticker mentions.

    Sources:
    - Reddit (requires REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET)
    - StockTwits trending (no auth needed)
    - Yahoo Finance trending (no auth needed)

    Steps:
    1. Scan all sources for ticker mentions
    2. Aggregate and rank mentions
    3. Store trending data to S3
    4. Optionally update watchlist

    Args:
        subreddits: List of subreddit names to scan
        lookback_hours: Hours to look back for Reddit posts
        min_mentions: Minimum mention count to include
        auto_update_watchlist: Whether to auto-update watchlist.json
        bucket: S3 bucket name (defaults to S3_BUCKET env var)
    """
    if bucket is None:
        bucket = resolve('s3-bucket', 'S3_BUCKET')
        if not bucket:
            print("[ERROR] S3_BUCKET not set")
            return {"error": "S3_BUCKET not set"}

    settings = load_settings()
    reddit_config = settings.get("reddit_scanner", {})

    if subreddits is None:
        subreddits = reddit_config.get(
            "subreddits", ["wallstreetbets", "stocks", "investing"]
        )

    lookback = reddit_config.get("scan_lookback_hours", lookback_hours)
    threshold = reddit_config.get("min_mention_threshold", min_mentions)

    print("\n" + "=" * 60)
    print("[SCANNER] Social Ticker Scanner")
    print("=" * 60)
    print(f"  Reddit: {', '.join(subreddits)}")
    print(f"  StockTwits: trending symbols")
    print(f"  Yahoo Finance: trending tickers")
    print(f"  Lookback: {lookback}h | Min mentions: {threshold}")
    print("=" * 60 + "\n")

    all_mentions = []

    # Source 1: Reddit
    reddit = create_reddit_client()
    for sub_name in subreddits:
        mentions = scan_subreddit(reddit, sub_name, lookback_hours=lookback)
        all_mentions.append((f"reddit/{sub_name}", mentions))

    # Source 2: StockTwits
    st_mentions = scan_stocktwits_trending()
    all_mentions.append(("stocktwits", st_mentions))

    # Source 3: Yahoo Finance
    yf_mentions = scan_yahoo_trending()
    all_mentions.append(("yahoo_finance", yf_mentions))

    # Aggregate all sources
    results = aggregate_mentions(all_mentions, min_mentions=threshold)

    # Store to S3
    s3_key = store_trending_to_s3(results, bucket)

    # Optionally update watchlist
    watchlist_summary = None
    if auto_update_watchlist:
        watchlist_summary = update_watchlist_from_trending(results)

    # Emit completion event
    emit_event(
        event="social.scanner.completed",
        resource={
            "prefect.resource.id": "social-ticker-scanner",
            "prefect.resource.name": "Social Ticker Scanner",
        },
        payload={
            "sources_scanned": len(all_mentions),
            "unique_tickers": results.get("total_unique_tickers", 0),
            "s3_key": s3_key,
            "watchlist_updated": auto_update_watchlist,
        },
    )

    print(f"\n[COMPLETE] Social scan finished: "
          f"{results.get('total_unique_tickers', 0)} tickers found")

    return {
        "trending": results,
        "s3_key": s3_key,
        "watchlist_update": watchlist_summary,
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    reddit_scanner_flow()
