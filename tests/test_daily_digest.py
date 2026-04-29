"""Tests for daily digest flow in flows/daily_digest.py"""

import pytest
import json
import io
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd

from flows.daily_digest import (
    gather_buy_signals,
    gather_reddit_trending,
    gather_price_movers,
    gather_new_watchlist_additions,
    compose_and_send_digest,
    create_digest_artifact,
    load_settings,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_signals():
    return [
        {
            "symbol": "NVDA",
            "signal": "Momentum Surge",
            "description": "7d momentum: 5.2%",
            "price": 875.50,
            "strength": "strong",
        },
        {
            "symbol": "AAPL",
            "signal": "Golden Cross",
            "description": "7d MA crossed above 30d MA",
            "price": 195.00,
            "strength": "strong",
        },
    ]


@pytest.fixture
def sample_trending():
    return [
        {"symbol": "NVDA", "mentions": 25},
        {"symbol": "PLTR", "mentions": 18},
        {"symbol": "SOFI", "mentions": 12},
    ]


@pytest.fixture
def sample_movers():
    return [
        {"symbol": "NVDA", "price": 875.50, "change_pct": 4.2, "volume": 35000000},
        {"symbol": "TSLA", "price": 250.00, "change_pct": -2.1, "volume": 50000000},
    ]


# ============================================================================
# Test Reddit Trending Loading
# ============================================================================

class TestGatherRedditTrending:
    """Test loading Reddit trending data from S3."""

    def test_load_trending_success(self):
        """Successfully loads trending data from S3."""
        mock_s3 = MagicMock()
        trending_data = {
            "tickers": {"NVDA": 25, "PLTR": 18, "SOFI": 12},
        }
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(trending_data).encode())
        }

        with patch("flows.daily_digest.make_boto3_client", return_value=mock_s3):
            result = gather_reddit_trending.fn(bucket="test-bucket", top_n=10)

        assert len(result) == 3
        assert result[0]["symbol"] == "NVDA"
        assert result[0]["mentions"] == 25

    def test_load_trending_no_data(self):
        """Returns empty list when no trending file exists."""
        mock_s3 = MagicMock()
        no_key_error = type("NoSuchKey", (Exception,), {})
        mock_s3.exceptions.NoSuchKey = no_key_error
        mock_s3.get_object.side_effect = no_key_error("Not found")

        with patch("flows.daily_digest.make_boto3_client", return_value=mock_s3):
            result = gather_reddit_trending.fn(bucket="test-bucket")

        assert result == []

    def test_load_trending_s3_error(self):
        """Returns empty list on S3 errors."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = Exception("Connection timeout")

        with patch("flows.daily_digest.make_boto3_client", return_value=mock_s3):
            result = gather_reddit_trending.fn(bucket="test-bucket")

        assert result == []


# ============================================================================
# Test Watchlist Additions
# ============================================================================

class TestGatherNewWatchlistAdditions:
    """Test new watchlist addition detection."""

    def test_additions_found(self):
        """Returns new additions when S3 history exists."""
        mock_s3 = MagicMock()
        history = {"date": "2026-04-27", "added": ["NVDA", "PLTR"]}
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(history).encode())
        }

        with patch("flows.daily_digest.make_boto3_client", return_value=mock_s3), \
             patch("flows.daily_digest.load_watchlist", return_value=["AAPL", "NVDA", "PLTR"]):
            result = gather_new_watchlist_additions.fn(bucket="test-bucket")

        assert result == ["NVDA", "PLTR"]

    def test_no_history_returns_empty(self):
        """Returns empty list when no history file exists."""
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("NoSuchKey")

        with patch("flows.daily_digest.make_boto3_client", return_value=mock_s3), \
             patch("flows.daily_digest.load_watchlist", return_value=["AAPL"]):
            result = gather_new_watchlist_additions.fn(bucket="test-bucket")

        assert result == []


# ============================================================================
# Test Email Sending
# ============================================================================

class TestComposeAndSendDigest:
    """Test digest email composition and sending."""

    def test_email_sent(self, sample_signals, sample_trending, sample_movers):
        """Email is sent when properly configured."""
        settings = {"alerts": {"email_enabled": True}}
        mock_smtp = MagicMock()

        with patch("flows.daily_digest.load_settings", return_value=settings), \
             patch("flows.daily_digest.resolve", side_effect=lambda p, e, **kw: {
                 "alert-email-from": "test@gmail.com",
                 "alert-email-to": "user@gmail.com",
                 "gmail-app-password": "testpass",
             }.get(p, "")), \
             patch("flows.daily_digest.smtplib.SMTP_SSL") as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            compose_and_send_digest.fn(sample_signals, sample_trending, sample_movers, [])

        mock_smtp.send_message.assert_called_once()

    def test_email_skipped_when_disabled(self, sample_signals, sample_trending, sample_movers):
        """Email is not sent when disabled in settings."""
        settings = {"alerts": {"email_enabled": False}}

        with patch("flows.daily_digest.load_settings", return_value=settings):
            compose_and_send_digest.fn(sample_signals, sample_trending, sample_movers, [])

    def test_email_skipped_missing_credentials(self, sample_signals, sample_trending, sample_movers):
        """Email is not sent when Gmail credentials are missing."""
        settings = {"alerts": {"email_enabled": True}}

        with patch("flows.daily_digest.load_settings", return_value=settings), \
             patch("flows.daily_digest.resolve", return_value=""):
            compose_and_send_digest.fn(sample_signals, sample_trending, sample_movers, [])

    def test_email_with_empty_data(self):
        """Email handles all-empty data gracefully."""
        settings = {"alerts": {"email_enabled": True}}
        mock_smtp = MagicMock()

        with patch("flows.daily_digest.load_settings", return_value=settings), \
             patch("flows.daily_digest.resolve", side_effect=lambda p, e, **kw: {
                 "alert-email-from": "test@gmail.com",
                 "alert-email-to": "user@gmail.com",
                 "gmail-app-password": "testpass",
             }.get(p, "")), \
             patch("flows.daily_digest.smtplib.SMTP_SSL") as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            compose_and_send_digest.fn([], [], [], [])

        mock_smtp.send_message.assert_called_once()


# ============================================================================
# Test Artifact Creation
# ============================================================================

class TestCreateDigestArtifact:
    """Test Prefect artifact creation."""

    def test_artifact_created_with_data(self, sample_signals, sample_trending, sample_movers):
        """Artifact is created with signal and trending content."""
        with patch("flows.daily_digest.create_markdown_artifact") as mock_artifact:
            create_digest_artifact.fn(sample_signals, sample_trending, sample_movers, ["NVDA"])

        mock_artifact.assert_called_once()
        call_kwargs = mock_artifact.call_args[1]
        assert "NVDA" in call_kwargs["markdown"]
        assert "Momentum Surge" in call_kwargs["markdown"]

    def test_artifact_created_empty(self):
        """Artifact is created even with no data."""
        with patch("flows.daily_digest.create_markdown_artifact") as mock_artifact:
            create_digest_artifact.fn([], [], [], [])

        mock_artifact.assert_called_once()
        call_kwargs = mock_artifact.call_args[1]
        assert "No signals" in call_kwargs["markdown"] or "Daily Digest" in call_kwargs["markdown"]


# ============================================================================
# Test Settings Loading
# ============================================================================

class TestLoadSettings:
    """Test settings file loading."""

    def test_load_settings_missing_file(self, tmp_path):
        """Returns empty dict when settings file doesn't exist."""
        with patch("flows.daily_digest.SETTINGS_PATH", tmp_path / "nonexistent.json"):
            result = load_settings()
        assert result == {}

    def test_load_settings_valid(self, tmp_path):
        """Returns parsed JSON when file exists."""
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{"daily_digest": {"enabled": true}}')
        with patch("flows.daily_digest.SETTINGS_PATH", settings_file):
            result = load_settings()
        assert result["daily_digest"]["enabled"] is True
