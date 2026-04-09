"""
Market Data Dashboard - Streamlit App
Displays daily analytics, ML features, volatility metrics, and buy signals.
Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dotenv

dotenv.load_dotenv()

# ============================================================================
# Configuration
# ============================================================================

S3_BUCKET = os.getenv("S3_BUCKET", "")
CONFIG_DIR = Path(__file__).parent.parent / "config"

st.set_page_config(
    page_title="Market Data Dashboard",
    page_icon="[CHART]",
    layout="wide",
)


# ============================================================================
# S3 Data Loading
# ============================================================================

@st.cache_data(ttl=300)
def load_parquet_from_s3(prefix):
    """Load all parquet files under an S3 prefix into a single DataFrame."""
    if not S3_BUCKET:
        st.error("S3_BUCKET environment variable not set")
        return pd.DataFrame()

    s3 = boto3.client("s3")
    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])

        if not keys:
            return pd.DataFrame()

        frames = []
        for key in keys:
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            buf = io.BytesIO(response["Body"].read())
            frames.append(pd.read_parquet(buf))

        return pd.concat(frames, ignore_index=True)

    except Exception as e:
        st.error(f"Failed to load data from S3: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_json_from_s3(key):
    """Load a JSON file from S3."""
    if not S3_BUCKET:
        return None

    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        return None


def load_watchlist():
    """Load watchlist from config file."""
    watchlist_path = CONFIG_DIR / "watchlist.json"
    if watchlist_path.exists():
        with open(watchlist_path, "r") as f:
            return json.load(f)
    return []


def load_settings():
    """Load pipeline settings."""
    settings_path = CONFIG_DIR / "pipeline_settings.json"
    if settings_path.exists():
        with open(settings_path, "r") as f:
            return json.load(f)
    return {}


# ============================================================================
# Navigation
# ============================================================================

PAGES = {
    "Summary": "summary",
    "Buy Signals": "buy_signals",
    "Volatility": "volatility",
    "Reddit Trending": "reddit_trending",
    "Portfolio Overview": "portfolio_overview",
}

st.sidebar.title("[CHART] Market Data")
page = st.sidebar.radio("Navigate", list(PAGES.keys()))

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Bucket:** `{S3_BUCKET or 'Not set'}`")
st.sidebar.markdown(f"**Updated:** {datetime.now().strftime('%H:%M:%S')}")
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ============================================================================
# Page: Summary
# ============================================================================

def page_summary():
    st.title("[CHART] Market Data Summary")

    watchlist = load_watchlist()
    if watchlist:
        st.subheader("Watchlist")
        cols = st.columns(min(len(watchlist), 6))
        for i, symbol in enumerate(watchlist):
            cols[i % len(cols)].metric(symbol, "---")

    # Daily analytics
    st.subheader("Daily Analytics")
    df = load_parquet_from_s3("analytics/daily_stats/")
    if df.empty:
        st.info("No daily analytics data found. Run the Spark analytics pipeline first.")
        return

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        latest_date = df["date"].max()
        latest = df[df["date"] == latest_date]
        st.caption(f"Latest data: {latest_date.strftime('%Y-%m-%d')}")
    else:
        latest = df

    if not latest.empty and "symbol" in latest.columns:
        st.dataframe(
            latest[["symbol", "avg_price", "min_price", "max_price",
                     "volatility", "total_volume"]].sort_values("symbol"),
            use_container_width=True,
            hide_index=True,
        )

        # Price chart
        if "avg_price" in latest.columns:
            fig = px.bar(
                latest.sort_values("avg_price", ascending=False),
                x="symbol",
                y="avg_price",
                title="Average Price by Symbol",
                color="avg_price",
                color_continuous_scale="blues",
            )
            st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# Page: Buy Signals
# ============================================================================

def page_buy_signals():
    st.title("[SIGNAL] Buy Signals")

    settings = load_settings()
    rules = settings.get("buy_signals", {}).get("rules", {})
    enabled = settings.get("buy_signals", {}).get("enabled", False)

    st.markdown(f"**Signal Detection:** {'Enabled' if enabled else 'Disabled'}")

    # Show active rules
    with st.expander("Active Rules"):
        for name, rule in rules.items():
            status = "[OK]" if rule.get("enabled") else "[OFF]"
            st.markdown(f"- {status} **{name}**: {rule.get('description', '')}")

    # Load ML features to detect current signals
    df = load_parquet_from_s3("analytics/ml_features/")
    if df.empty:
        st.info("No ML features data found. Run the Spark analytics pipeline first.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    latest = df.sort_values("timestamp").groupby("symbol").tail(2)

    signals = []
    for symbol in latest["symbol"].unique():
        sym_data = latest[latest["symbol"] == symbol].sort_values("timestamp")
        if len(sym_data) < 2:
            continue
        prev = sym_data.iloc[-2]
        curr = sym_data.iloc[-1]

        if rules.get("golden_cross", {}).get("enabled"):
            if prev.get("ma_7d", 0) <= prev.get("ma_30d", 0) and curr.get("ma_7d", 0) > curr.get("ma_30d", 0):
                signals.append({"Symbol": symbol, "Signal": "Golden Cross",
                                "Price": f"${curr.get('price', 0):.2f}", "Strength": "Strong"})

        min_mom = rules.get("momentum_surge", {}).get("min_momentum_pct", 3.0)
        if rules.get("momentum_surge", {}).get("enabled"):
            mom = curr.get("price_momentum_7d", 0)
            if mom and mom >= min_mom:
                strength = "Strong" if mom >= min_mom * 2 else "Moderate"
                signals.append({"Symbol": symbol, "Signal": "Momentum Surge",
                                "Price": f"${curr.get('price', 0):.2f}", "Strength": strength})

        if rules.get("oversold_bounce", {}).get("enabled"):
            if prev.get("price", 0) < prev.get("ma_30d", 0) and curr.get("price", 0) > curr.get("ma_30d", 0):
                signals.append({"Symbol": symbol, "Signal": "Oversold Bounce",
                                "Price": f"${curr.get('price', 0):.2f}", "Strength": "Moderate"})

        max_vol = rules.get("low_vol_uptrend", {}).get("max_volatility_7d_pct", 2.0)
        min_m = rules.get("low_vol_uptrend", {}).get("min_momentum_pct", 1.0)
        if rules.get("low_vol_uptrend", {}).get("enabled"):
            vol = curr.get("volatility_7d")
            mom = curr.get("price_momentum_7d", 0)
            if vol is not None and vol < max_vol and mom and mom >= min_m:
                signals.append({"Symbol": symbol, "Signal": "Low Vol Uptrend",
                                "Price": f"${curr.get('price', 0):.2f}", "Strength": "Moderate"})

    if signals:
        st.subheader(f"Active Signals ({len(signals)})")
        st.dataframe(pd.DataFrame(signals), use_container_width=True, hide_index=True)
    else:
        st.info("No active buy signals detected.")

    # Signal history - show ML features over time
    st.subheader("ML Features History")
    symbols = df["symbol"].unique().tolist()
    selected = st.selectbox("Select symbol", symbols)
    if selected:
        sym_df = df[df["symbol"] == selected].sort_values("timestamp")
        if not sym_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=sym_df["timestamp"], y=sym_df["price"], name="Price"))
            if "ma_7d" in sym_df.columns:
                fig.add_trace(go.Scatter(x=sym_df["timestamp"], y=sym_df["ma_7d"], name="7d MA"))
            if "ma_30d" in sym_df.columns:
                fig.add_trace(go.Scatter(x=sym_df["timestamp"], y=sym_df["ma_30d"], name="30d MA"))
            fig.update_layout(title=f"{selected} - Price & Moving Averages", xaxis_title="Date", yaxis_title="Price")
            st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# Page: Volatility
# ============================================================================

def page_volatility():
    st.title("[WAVE] Volatility Metrics")

    df = load_parquet_from_s3("analytics/volatility_metrics/")
    if df.empty:
        st.info("No volatility data found. Run the Spark analytics pipeline first.")
        return

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        latest_date = df["date"].max()
        latest = df[df["date"] == latest_date]
        st.caption(f"Latest data: {latest_date.strftime('%Y-%m-%d')}")
    else:
        latest = df

    # Volatility rankings
    st.subheader("Volatility Rankings")
    if "annualized_volatility" in latest.columns:
        ranked = latest.sort_values("annualized_volatility", ascending=False)
        st.dataframe(
            ranked[["symbol", "daily_volatility", "annualized_volatility",
                     "atr_approx", "large_moves_count"]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

        fig = px.bar(
            ranked,
            x="symbol",
            y="annualized_volatility",
            title="Annualized Volatility by Symbol",
            color="annualized_volatility",
            color_continuous_scale="reds",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ATR chart
    if "atr_approx" in latest.columns:
        st.subheader("ATR (Average True Range)")
        fig = px.bar(
            latest.sort_values("atr_approx", ascending=False),
            x="symbol",
            y="atr_approx",
            title="ATR Approximation by Symbol",
            color="atr_approx",
            color_continuous_scale="oranges",
        )
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# Page: Reddit Trending
# ============================================================================

def page_reddit_trending():
    st.title("[FIRE] Reddit Trending Tickers")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = load_json_from_s3(f"reddit/trending/{today}.json")

    if data is None:
        # Try yesterday
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        data = load_json_from_s3(f"reddit/trending/{yesterday}.json")
        if data is None:
            st.info("No Reddit trending data found. Run the Reddit scanner flow first.")
            return
        st.caption(f"Showing data from: {yesterday}")
    else:
        st.caption(f"Scan date: {today}")

    watchlist = set(load_watchlist())
    tickers = data.get("tickers", {})
    top_20 = data.get("top_20", {})

    if not top_20:
        st.info("No trending tickers above threshold.")
        return

    # Top 20 bar chart
    st.subheader("Top 20 Mentioned Tickers")
    top_df = pd.DataFrame(
        [{"Ticker": k, "Mentions": v, "In Watchlist": k in watchlist}
         for k, v in sorted(top_20.items(), key=lambda x: x[1], reverse=True)]
    )

    fig = px.bar(
        top_df,
        x="Ticker",
        y="Mentions",
        color="In Watchlist",
        color_discrete_map={True: "#2e7d32", False: "#1565c0"},
        title="Top 20 Reddit Ticker Mentions",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Detailed table
    st.subheader("Detailed Mentions")
    detail_rows = []
    by_sub = data.get("by_subreddit", {})
    for ticker, count in sorted(tickers.items(), key=lambda x: x[1], reverse=True)[:20]:
        row = {"Ticker": ticker, "Total": count, "Watchlist": "[YES]" if ticker in watchlist else ""}
        for sub_name, sub_data in by_sub.items():
            row[f"r/{sub_name}"] = sub_data.get(ticker, 0)
        detail_rows.append(row)

    if detail_rows:
        st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)

    # Historical trending (last 7 days)
    st.subheader("Trending Over Time (Last 7 Days)")
    history = {}
    for days_ago in range(7):
        date = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        day_data = load_json_from_s3(f"reddit/trending/{date}.json")
        if day_data:
            history[date] = day_data.get("top_20", {})

    if len(history) > 1:
        # Build time series for top tickers
        all_tickers_in_history = set()
        for day_tickers in history.values():
            all_tickers_in_history.update(list(day_tickers.keys())[:10])

        rows = []
        for date, day_tickers in sorted(history.items()):
            for ticker in all_tickers_in_history:
                rows.append({"Date": date, "Ticker": ticker, "Mentions": day_tickers.get(ticker, 0)})

        hist_df = pd.DataFrame(rows)
        fig = px.line(
            hist_df,
            x="Date",
            y="Mentions",
            color="Ticker",
            title="Ticker Mentions Over Last 7 Days",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough historical data for trend chart (need 2+ days).")


# ============================================================================
# Page: Portfolio Overview
# ============================================================================

def page_portfolio_overview():
    st.title("[FOLDER] Portfolio Overview")

    watchlist = load_watchlist()
    if not watchlist:
        st.warning("No symbols in watchlist.")
        return

    # Load all data sources
    daily_df = load_parquet_from_s3("analytics/daily_stats/")
    ml_df = load_parquet_from_s3("analytics/ml_features/")
    vol_df = load_parquet_from_s3("analytics/volatility_metrics/")

    if daily_df.empty and ml_df.empty and vol_df.empty:
        st.info("No analytics data found. Run the pipeline first.")
        return

    # Get latest data per symbol
    daily_latest = {}
    if not daily_df.empty and "date" in daily_df.columns:
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        for _, row in daily_df.sort_values("date").groupby("symbol").tail(1).iterrows():
            daily_latest[row["symbol"]] = row

    ml_latest = {}
    if not ml_df.empty and "timestamp" in ml_df.columns:
        ml_df["timestamp"] = pd.to_datetime(ml_df["timestamp"])
        for _, row in ml_df.sort_values("timestamp").groupby("symbol").tail(1).iterrows():
            ml_latest[row["symbol"]] = row

    vol_latest = {}
    if not vol_df.empty and "date" in vol_df.columns:
        vol_df["date"] = pd.to_datetime(vol_df["date"])
        for _, row in vol_df.sort_values("date").groupby("symbol").tail(1).iterrows():
            vol_latest[row["symbol"]] = row

    # Data freshness
    timestamps = []
    if not daily_df.empty and "date" in daily_df.columns:
        timestamps.append(("Daily Stats", daily_df["date"].max()))
    if not ml_df.empty and "timestamp" in ml_df.columns:
        timestamps.append(("ML Features", ml_df["timestamp"].max()))
    if not vol_df.empty and "date" in vol_df.columns:
        timestamps.append(("Volatility", vol_df["date"].max()))

    if timestamps:
        cols = st.columns(len(timestamps))
        for i, (name, ts) in enumerate(timestamps):
            cols[i].metric(f"{name} Last Updated", str(ts)[:10])

    st.markdown("---")

    # Build overview table
    overview_rows = []
    for symbol in watchlist:
        row = {"Symbol": symbol}

        d = daily_latest.get(symbol)
        if d is not None:
            row["Price"] = f"${d.get('avg_price', 0):.2f}"
            row["Volume"] = f"{int(d.get('total_volume', 0)):,}"
        else:
            row["Price"] = "---"
            row["Volume"] = "---"

        m = ml_latest.get(symbol)
        if m is not None:
            mom = m.get("price_momentum_7d", 0)
            row["7d Momentum"] = f"{mom:.1f}%" if mom else "---"
            ma7 = m.get("ma_7d", 0)
            ma30 = m.get("ma_30d", 0)
            if ma7 and ma30:
                row["MA Signal"] = "Bullish" if ma7 > ma30 else "Bearish"
            else:
                row["MA Signal"] = "---"
        else:
            row["7d Momentum"] = "---"
            row["MA Signal"] = "---"

        v = vol_latest.get(symbol)
        if v is not None:
            ann_vol = v.get("annualized_volatility", 0)
            row["Ann. Volatility"] = f"{ann_vol:.1f}%" if ann_vol else "---"
            # Signal strength color coding
            if ann_vol and ann_vol < 20:
                row["Risk"] = "Low"
            elif ann_vol and ann_vol < 40:
                row["Risk"] = "Medium"
            else:
                row["Risk"] = "High"
        else:
            row["Ann. Volatility"] = "---"
            row["Risk"] = "---"

        overview_rows.append(row)

    if overview_rows:
        st.subheader("Symbol Overview")
        overview_df = pd.DataFrame(overview_rows)
        st.dataframe(overview_df, use_container_width=True, hide_index=True)

    # Individual symbol cards
    st.subheader("Symbol Details")
    selected = st.selectbox("Select symbol", watchlist)
    if selected:
        col1, col2, col3 = st.columns(3)

        d = daily_latest.get(selected)
        if d is not None:
            col1.metric("Avg Price", f"${d.get('avg_price', 0):.2f}")
            col1.metric("Day Range", f"${d.get('min_price', 0):.2f} - ${d.get('max_price', 0):.2f}")
            col1.metric("Volume", f"{int(d.get('total_volume', 0)):,}")

        m = ml_latest.get(selected)
        if m is not None:
            col2.metric("7d MA", f"${m.get('ma_7d', 0):.2f}")
            col2.metric("30d MA", f"${m.get('ma_30d', 0):.2f}")
            col2.metric("7d Momentum", f"{m.get('price_momentum_7d', 0):.1f}%")

        v = vol_latest.get(selected)
        if v is not None:
            col3.metric("Daily Volatility", f"{v.get('daily_volatility', 0):.2f}%")
            col3.metric("Annualized Vol", f"{v.get('annualized_volatility', 0):.1f}%")
            col3.metric("ATR", f"${v.get('atr_approx', 0):.2f}")


# ============================================================================
# Route pages
# ============================================================================

if PAGES[page] == "summary":
    page_summary()
elif PAGES[page] == "buy_signals":
    page_buy_signals()
elif PAGES[page] == "volatility":
    page_volatility()
elif PAGES[page] == "reddit_trending":
    page_reddit_trending()
elif PAGES[page] == "portfolio_overview":
    page_portfolio_overview()
