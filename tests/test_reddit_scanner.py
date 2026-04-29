"""Tests for Reddit ticker scanner flow in flows/reddit_scanner.py"""

import pytest
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from collections import Counter

from flows.reddit_scanner import (
    extract_tickers,
    aggregate_mentions,
    scan_subreddit,
    store_trending_to_s3,
    update_watchlist_from_trending,
    load_settings,
    load_watchlist,
)


# ============================================================================
# Ticker Extraction Tests
# ============================================================================

class TestExtractTickers:
    """Test ticker extraction regex and filtering."""

    def test_dollar_sign_tickers(self):
        """$AAPL style tickers are extracted."""
        result = extract_tickers("I'm buying $AAPL and $TSLA today")
        assert "AAPL" in result
        assert "TSLA" in result

    def test_bare_uppercase_tickers(self):
        """Bare uppercase words (2+ chars) are extracted."""
        result = extract_tickers("Looking at MSFT and GOOGL")
        assert "MSFT" in result
        assert "GOOGL" in result

    def test_single_char_dollar_tickers(self):
        """Single char tickers with $ prefix are extracted (e.g., $F)."""
        result = extract_tickers("$F is looking good")
        assert "F" in result

    def test_single_char_bare_tickers_filtered(self):
        """Single char bare words are NOT extracted (too many false positives)."""
        result = extract_tickers("I like F stock")
        assert "F" not in result

    def test_false_positives_filtered(self):
        """Common false positives are filtered out."""
        result = extract_tickers("The CEO said IPO is coming. YOLO on this DD!")
        assert "CEO" not in result
        assert "IPO" not in result
        assert "YOLO" not in result
        assert "DD" not in result

    def test_mixed_case_ignored(self):
        """Mixed case words are not matched (regex requires all uppercase)."""
        result = extract_tickers("Apple is great, Tesla too")
        assert "Apple" not in result
        assert "Tesla" not in result

    def test_no_duplicates(self):
        """Same ticker mentioned twice doesn't duplicate."""
        result = extract_tickers("$AAPL is great. AAPL to the moon!")
        assert result.count("AAPL") == 1

    def test_empty_text(self):
        """Empty text returns empty list."""
        assert extract_tickers("") == []

    def test_no_tickers_in_lowercase(self):
        """All lowercase text returns nothing."""
        assert extract_tickers("this is just some regular text") == []

    def test_ticker_max_length(self):
        """Tickers longer than 5 chars are not matched."""
        result = extract_tickers("LONGERTICKER is not valid")
        assert "LONGERTICKER" not in result

    def test_common_words_filtered(self):
        """Common English words that look like tickers are filtered."""
        text = "THE BEST NEXT REAL HUGE OPEN CASH FREE SAFE RISK"
        result = extract_tickers(text)
        assert len(result) == 0


# ============================================================================
# Mention Counting and Aggregation Tests
# ============================================================================

class TestAggregateMentions:
    """Test mention counting and ranking."""

    def test_basic_aggregation(self):
        """Mentions from multiple subreddits are combined."""
        mentions = [
            ("wsb", Counter({"AAPL": 10, "TSLA": 5})),
            ("stocks", Counter({"AAPL": 3, "MSFT": 7})),
        ]
        result = aggregate_mentions.fn(mentions, min_mentions=2)

        assert result["tickers"]["AAPL"] == 13
        assert result["tickers"]["MSFT"] == 7
        assert result["tickers"]["TSLA"] == 5

    def test_min_mentions_filter(self):
        """Tickers below min_mentions threshold are excluded."""
        mentions = [
            ("wsb", Counter({"AAPL": 10, "RARE": 1})),
        ]
        result = aggregate_mentions.fn(mentions, min_mentions=5)

        assert "AAPL" in result["tickers"]
        assert "RARE" not in result["tickers"]

    def test_top_20_included(self):
        """Top 20 field is populated."""
        mentions = [
            ("wsb", Counter({f"T{i}": 100 - i for i in range(25)})),
        ]
        result = aggregate_mentions.fn(mentions, min_mentions=1)
        assert len(result["top_20"]) == 20

    def test_by_subreddit_breakdown(self):
        """Per-subreddit breakdown is included."""
        mentions = [
            ("wsb", Counter({"AAPL": 10})),
            ("stocks", Counter({"MSFT": 5})),
        ]
        result = aggregate_mentions.fn(mentions, min_mentions=1)

        assert "wsb" in result["by_subreddit"]
        assert "stocks" in result["by_subreddit"]
        assert result["by_subreddit"]["wsb"]["AAPL"] == 10

    def test_empty_mentions(self):
        """Empty mentions list returns zero tickers."""
        result = aggregate_mentions.fn([], min_mentions=1)
        assert result["total_unique_tickers"] == 0
        assert result["tickers"] == {}

    def test_scan_timestamp_present(self):
        """Result includes scan timestamp."""
        result = aggregate_mentions.fn([], min_mentions=1)
        assert "scan_timestamp" in result


# ============================================================================
# Subreddit Scanning Tests (Mocked)
# ============================================================================

class TestScanSubreddit:
    """Test subreddit scanning with mocked requests.Session (JSON API)."""

    def _make_mock_session(self, posts_json, comments_json=None):
        """Build a mock session that returns posts and optionally comments."""
        session = MagicMock()

        if comments_json is None:
            comments_json = [{"data": {"children": []}}, {"data": {"children": []}}]

        post_resp = MagicMock()
        post_resp.json.return_value = {"data": {"children": posts_json}}
        post_resp.raise_for_status = MagicMock()

        comment_resp = MagicMock()
        comment_resp.json.return_value = comments_json
        comment_resp.raise_for_status = MagicMock()

        def get_side_effect(url, **kwargs):
            if "/comments/" in url:
                return comment_resp
            return post_resp

        session.get.side_effect = get_side_effect
        return session

    @patch("flows.reddit_scanner.load_ignore_list", return_value=set())
    @patch("flows.reddit_scanner.time")
    def test_scan_returns_counter(self, mock_time, mock_ignore):
        """Scanning returns a Counter of ticker mentions."""
        posts = [{"data": {
            "id": "abc",
            "title": "Buy $AAPL now!",
            "selftext": "TSLA is also good",
            "created_utc": datetime.now(timezone.utc).timestamp(),
        }}]
        session = self._make_mock_session(posts)
        result = scan_subreddit.fn(session, "wallstreetbets", lookback_hours=24)

        assert isinstance(result, Counter)
        assert "AAPL" in result
        assert "TSLA" in result

    @patch("flows.reddit_scanner.load_ignore_list", return_value=set())
    @patch("flows.reddit_scanner.time")
    def test_scan_filters_old_posts(self, mock_time, mock_ignore):
        """Posts older than lookback_hours are skipped."""
        posts = [{"data": {
            "id": "old1",
            "title": "$AAPL old post",
            "selftext": "",
            "created_utc": (datetime.now(timezone.utc) - timedelta(hours=48)).timestamp(),
        }}]
        session = self._make_mock_session(posts)
        result = scan_subreddit.fn(session, "stocks", lookback_hours=24)
        assert "AAPL" not in result

    @patch("flows.reddit_scanner.load_ignore_list", return_value=set())
    @patch("flows.reddit_scanner.time")
    def test_scan_includes_comments(self, mock_time, mock_ignore):
        """Ticker mentions in comments are counted."""
        posts = [{"data": {
            "id": "post1",
            "title": "Market discussion",
            "selftext": "",
            "created_utc": datetime.now(timezone.utc).timestamp(),
        }}]
        comments_json = [
            {"data": {"children": []}},
            {"data": {"children": [
                {"data": {"body": "$NVDA to the moon"}},
            ]}},
        ]
        session = self._make_mock_session(posts, comments_json)
        result = scan_subreddit.fn(session, "investing", lookback_hours=24)
        assert "NVDA" in result

    @patch("flows.reddit_scanner.load_ignore_list", return_value=set())
    def test_scan_handles_api_error(self, mock_ignore):
        """API errors are caught and return empty Counter."""
        session = MagicMock()
        session.get.side_effect = Exception("Reddit API error")

        result = scan_subreddit.fn(session, "wallstreetbets", lookback_hours=24)
        assert isinstance(result, Counter)
        assert len(result) == 0


# ============================================================================
# Watchlist Merge Tests
# ============================================================================

class TestUpdateWatchlist:
    """Test watchlist merge logic."""

    def test_new_tickers_added(self, tmp_path):
        """New tickers above threshold are added to watchlist."""
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('["AAPL", "MSFT"]')
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{}')

        results = {
            "tickers": {"NVDA": 50, "TSLA": 30, "AAPL": 20},
        }

        with patch("flows.reddit_scanner.WATCHLIST_PATH", watchlist_file), \
             patch("flows.reddit_scanner.SETTINGS_PATH", settings_file), \
             patch("flows.reddit_scanner.create_markdown_artifact"), \
             patch("flows.reddit_scanner.resolve", return_value=""), \
             patch("flows.reddit_scanner.make_boto3_client"):
            summary = update_watchlist_from_trending.fn(results, auto_add_top_n=5, min_mentions=10)

        assert len(summary["added"]) == 2  # NVDA and TSLA
        added_tickers = {e["ticker"] for e in summary["added"]}
        assert "NVDA" in added_tickers
        assert "TSLA" in added_tickers

        # Verify file was updated
        updated = json.loads(watchlist_file.read_text())
        assert "NVDA" in updated
        assert "TSLA" in updated

    def test_existing_tickers_skipped(self, tmp_path):
        """Tickers already in watchlist are skipped."""
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('["AAPL", "TSLA"]')
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{}')

        results = {"tickers": {"AAPL": 50, "TSLA": 30}}

        with patch("flows.reddit_scanner.WATCHLIST_PATH", watchlist_file), \
             patch("flows.reddit_scanner.SETTINGS_PATH", settings_file), \
             patch("flows.reddit_scanner.create_markdown_artifact"), \
             patch("flows.reddit_scanner.resolve", return_value=""), \
             patch("flows.reddit_scanner.make_boto3_client"):
            summary = update_watchlist_from_trending.fn(results, auto_add_top_n=5, min_mentions=10)

        assert len(summary["added"]) == 0
        skipped_reasons = {e["reason"] for e in summary["skipped"]}
        assert "already_in_watchlist" in skipped_reasons

    def test_below_threshold_skipped(self, tmp_path):
        """Tickers below min_mentions are skipped."""
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('[]')
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{}')

        results = {"tickers": {"NVDA": 5}}

        with patch("flows.reddit_scanner.WATCHLIST_PATH", watchlist_file), \
             patch("flows.reddit_scanner.SETTINGS_PATH", settings_file), \
             patch("flows.reddit_scanner.create_markdown_artifact"), \
             patch("flows.reddit_scanner.resolve", return_value=""), \
             patch("flows.reddit_scanner.make_boto3_client"):
            summary = update_watchlist_from_trending.fn(results, auto_add_top_n=5, min_mentions=10)

        assert len(summary["added"]) == 0
        assert summary["skipped"][0]["reason"] == "below_threshold"

    def test_manual_tickers_preserved(self, tmp_path):
        """Existing manual tickers are never removed."""
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('["AAPL", "GOOGL", "BTI"]')
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{}')

        results = {"tickers": {"NVDA": 50}}

        with patch("flows.reddit_scanner.WATCHLIST_PATH", watchlist_file), \
             patch("flows.reddit_scanner.SETTINGS_PATH", settings_file), \
             patch("flows.reddit_scanner.create_markdown_artifact"), \
             patch("flows.reddit_scanner.resolve", return_value=""), \
             patch("flows.reddit_scanner.make_boto3_client"):
            update_watchlist_from_trending.fn(results, auto_add_top_n=5, min_mentions=10)

        updated = json.loads(watchlist_file.read_text())
        assert "AAPL" in updated
        assert "GOOGL" in updated
        assert "BTI" in updated
        assert "NVDA" in updated

    def test_respects_top_n_limit(self, tmp_path):
        """Only top N tickers are considered."""
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('[]')
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{}')

        results = {
            "tickers": {
                "T1": 100, "T2": 90, "T3": 80,
                "T4": 70, "T5": 60, "T6": 50,
            }
        }

        with patch("flows.reddit_scanner.WATCHLIST_PATH", watchlist_file), \
             patch("flows.reddit_scanner.SETTINGS_PATH", settings_file), \
             patch("flows.reddit_scanner.create_markdown_artifact"), \
             patch("flows.reddit_scanner.resolve", return_value=""), \
             patch("flows.reddit_scanner.make_boto3_client"):
            summary = update_watchlist_from_trending.fn(results, auto_add_top_n=3, min_mentions=10)

        # Only top 3 should be considered
        added_tickers = {e["ticker"] for e in summary["added"]}
        assert len(added_tickers) == 3
        assert "T4" not in added_tickers


# ============================================================================
# S3 Storage Tests (Mocked)
# ============================================================================

class TestStoreTrendingToS3:
    """Test S3 storage of trending data."""

    def test_store_to_s3(self):
        """Trending data is stored to correct S3 path."""
        mock_s3 = MagicMock()
        results = {"tickers": {"AAPL": 10}, "total_unique_tickers": 1}

        with patch("flows.reddit_scanner.make_boto3_client", return_value=mock_s3):
            s3_key = store_trending_to_s3.fn(results, bucket="test-bucket")

        assert s3_key.startswith("reddit/trending/")
        assert s3_key.endswith(".json")
        mock_s3.put_object.assert_called_once()

        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["ContentType"] == "application/json"

    def test_store_s3_error_raises(self):
        """S3 errors are propagated."""
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("S3 error")

        with patch("flows.reddit_scanner.make_boto3_client", return_value=mock_s3):
            with pytest.raises(Exception, match="S3 error"):
                store_trending_to_s3.fn({"tickers": {}}, bucket="test-bucket")


# ============================================================================
# Edge Cases
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""

    @patch("flows.reddit_scanner.load_ignore_list", return_value=set())
    def test_empty_reddit_response(self, mock_ignore):
        """Empty subreddit (no posts) returns empty Counter."""
        session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {"data": {"children": []}}
        resp.raise_for_status = MagicMock()
        session.get.return_value = resp

        result = scan_subreddit.fn(session, "empty_sub", lookback_hours=24)
        assert isinstance(result, Counter)
        assert len(result) == 0

    def test_no_mentions_above_threshold(self):
        """When no tickers meet min_mentions, result has zero tickers."""
        mentions = [("wsb", Counter({"AAPL": 1, "TSLA": 1}))]
        result = aggregate_mentions.fn(mentions, min_mentions=5)
        assert result["total_unique_tickers"] == 0

    def test_extract_tickers_special_characters(self):
        """Tickers are extracted correctly around punctuation."""
        result = extract_tickers("$AAPL, $TSLA. MSFT! GOOGL?")
        assert "AAPL" in result
        assert "TSLA" in result
        assert "MSFT" in result
        assert "GOOGL" in result

    def test_load_settings_missing_file(self, tmp_path):
        """Returns empty dict when settings file doesn't exist."""
        with patch("flows.reddit_scanner.SETTINGS_PATH", tmp_path / "nonexistent.json"):
            result = load_settings()
        assert result == {}

    def test_load_watchlist_missing_file(self, tmp_path):
        """Returns empty list when watchlist file doesn't exist."""
        with patch("flows.reddit_scanner.WATCHLIST_PATH", tmp_path / "nonexistent.json"):
            result = load_watchlist()
        assert result == []
