"""
Reddit Ticker Scanner - Prefect Flow
Scans Reddit communities for stock ticker mentions and stores trending data to S3.
"""

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
import boto3
import praw
import re
import json
import os
from datetime import datetime, timedelta
from collections import Counter
from pathlib import Path

import dotenv

dotenv.load_dotenv()

# ============================================================================
# Configuration
# ============================================================================

SETTINGS_PATH = Path(__file__).parent.parent / "config" / "pipeline_settings.json"
WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "watchlist.json"
IGNORE_LIST_PATH = Path(__file__).parent.parent / "config" / "reddit_ignore_list.json"


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
    Create an authenticated Reddit client using PRAW.

    Requires env vars:
    - REDDIT_CLIENT_ID
    - REDDIT_CLIENT_SECRET
    - REDDIT_USER_AGENT

    Returns:
        praw.Reddit: Authenticated Reddit instance
    """
    client_id = os.getenv("REDDIT_CLIENT_ID", "")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
    user_agent = os.getenv("REDDIT_USER_AGENT", "market-data-scanner/1.0")

    if not client_id or not client_secret:
        raise ValueError(
            "Reddit API credentials not set. "
            "Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in .env"
        )

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    print("[OK] Reddit client created")
    return reddit


@task(log_prints=True, tags=["reddit", "scan"])
def scan_subreddit(reddit, subreddit_name, lookback_hours=24, post_limit=100):
    """
    Scan a subreddit for ticker mentions.

    Args:
        reddit: Authenticated PRAW Reddit instance
        subreddit_name: Name of subreddit to scan (without r/)
        lookback_hours: How far back to look for posts
        post_limit: Maximum number of posts to scan

    Returns:
        Counter: Ticker mention counts from this subreddit
    """
    print(f"[INFO] Scanning r/{subreddit_name} (last {lookback_hours}h)...")
    mentions = Counter()
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    ignore_set = load_ignore_list()

    if ignore_set:
        print(f"  [INFO] Ignoring {len(ignore_set)} tickers from ignore list")

    try:
        subreddit = reddit.subreddit(subreddit_name)
        posts_scanned = 0

        for post in subreddit.new(limit=post_limit):
            post_time = datetime.utcfromtimestamp(post.created_utc)
            if post_time < cutoff:
                continue

            posts_scanned += 1
            text = f"{post.title} {post.selftext}"
            tickers = extract_tickers(text, ignore_set=ignore_set)
            for ticker in tickers:
                mentions[ticker] += 1

            # Also scan top-level comments
            post.comments.replace_more(limit=0)
            for comment in post.comments[:20]:
                comment_tickers = extract_tickers(comment.body, ignore_set=ignore_set)
                for ticker in comment_tickers:
                    mentions[ticker] += 1

        print(f"  [OK] r/{subreddit_name}: scanned {posts_scanned} posts, "
              f"found {len(mentions)} unique tickers")

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
        "scan_timestamp": datetime.utcnow().isoformat(),
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
    s3 = boto3.client("s3")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
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
**Timestamp:** {datetime.utcnow().isoformat()}

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
        key=f"watchlist-update-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        markdown=artifact_md,
        description="Watchlist auto-update from Reddit trending",
    )

    return summary


# ============================================================================
# Flow
# ============================================================================

@flow(
    name="reddit-ticker-scanner",
    description="Scan Reddit communities for trending stock ticker mentions",
    log_prints=True,
)
def reddit_scanner_flow(
    subreddits=None,
    lookback_hours=24,
    min_mentions=2,
    auto_update_watchlist=True,
    bucket=None,
):
    """
    Scan Reddit for stock ticker mentions and store results.

    Steps:
    1. Create Reddit client
    2. Scan configured subreddits
    3. Aggregate and rank mentions
    4. Store trending data to S3
    5. Optionally update watchlist

    Args:
        subreddits: List of subreddit names to scan
        lookback_hours: Hours to look back for posts
        min_mentions: Minimum mention count to include
        auto_update_watchlist: Whether to auto-update watchlist.json
        bucket: S3 bucket name (defaults to S3_BUCKET env var)
    """
    if bucket is None:
        bucket = os.getenv("S3_BUCKET", "")
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
    print("[SCANNER] Reddit Ticker Scanner")
    print("=" * 60)
    print(f"  Subreddits: {', '.join(subreddits)}")
    print(f"  Lookback: {lookback}h")
    print(f"  Min mentions: {threshold}")
    print("=" * 60 + "\n")

    # Step 1: Create Reddit client
    reddit = create_reddit_client()

    # Step 2: Scan each subreddit
    subreddit_mentions = []
    for sub_name in subreddits:
        mentions = scan_subreddit(reddit, sub_name, lookback_hours=lookback)
        subreddit_mentions.append((sub_name, mentions))

    # Step 3: Aggregate
    results = aggregate_mentions(subreddit_mentions, min_mentions=threshold)

    # Step 4: Store to S3
    s3_key = store_trending_to_s3(results, bucket)

    # Step 5: Optionally update watchlist
    watchlist_summary = None
    if auto_update_watchlist:
        watchlist_summary = update_watchlist_from_trending(results)

    # Emit completion event
    emit_event(
        event="reddit.scanner.completed",
        resource={
            "prefect.resource.id": "reddit-ticker-scanner",
            "prefect.resource.name": "Reddit Ticker Scanner",
        },
        payload={
            "subreddits_scanned": len(subreddits),
            "unique_tickers": results.get("total_unique_tickers", 0),
            "s3_key": s3_key,
            "watchlist_updated": auto_update_watchlist,
        },
    )

    print(f"\n[COMPLETE] Reddit scan finished: "
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
