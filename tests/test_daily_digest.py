"""Tests for daily digest flow with Claude AI synopsis generation."""

import pytest
import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import io

from flows.daily_digest import (
    generate_claude_synopsis,
    load_reddit_trending,
    gather_stock_context,
    send_digest_email,
    create_digest_artifact,
    load_settings,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def digest_settings():
    """Pipeline settings with daily digest enabled."""
    return {
        "daily_digest": {
            "enabled": True,
            "claude_model": "claude-sonnet-4-20250514",
            "max_tokens_per_stock": 500,
            "max_stocks": 10,
        },
        "alerts": {"email_enabled": False},
    }


@pytest.fixture
def digest_settings_disabled():
    """Pipeline settings with daily digest disabled."""
    return {
        "daily_digest": {"enabled": False},
        "alerts": {"email_enabled": False},
    }


@pytest.fixture
def sample_trending_data():
    """Sample Reddit trending data as stored in S3."""
    return {
        "scan_timestamp": "2025-01-15T12:00:00",
        "total_unique_tickers": 3,
        "tickers": {"NVDA": 25, "PLTR": 18, "SOFI": 12},
        "top_20": {"NVDA": 25, "PLTR": 18, "SOFI": 12},
        "by_subreddit": {
            "wallstreetbets": {"NVDA": 15, "PLTR": 10, "SOFI": 8},
            "stocks": {"NVDA": 10, "PLTR": 8, "SOFI": 4},
        },
    }


@pytest.fixture
def sample_stock_context():
    """Sample analytics context for a ticker."""
    return {
        "symbol": "NVDA",
        "price": 875.50,
        "ma_7d": 860.00,
        "ma_30d": 820.00,
        "momentum_7d": 5.2,
        "volatility_7d": 2.1,
        "volatility_30d": 3.5,
        "annualized_volatility": 45.0,
        "daily_volume": 35000000,
        "daily_range_pct": 3.5,
        "large_move_count": 8,
    }


# ============================================================================
# Test Reddit Trending Loading
# ============================================================================

class TestLoadRedditTrending:
    """Test loading Reddit trending data from S3."""

    def test_load_trending_success(self, sample_trending_data):
        """Successfully loads trending data from S3."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(sample_trending_data).encode())
        }
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

        with patch("flows.daily_digest.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_s3
            result = load_reddit_trending.fn(bucket="test-bucket")

        assert result["total_unique_tickers"] == 3
        assert result["tickers"]["NVDA"] == 25

    def test_load_trending_no_data(self):
        """Returns empty dict when no trending file exists."""
        mock_s3 = MagicMock()
        no_key_error = type("NoSuchKey", (Exception,), {})
        mock_s3.exceptions.NoSuchKey = no_key_error
        mock_s3.get_object.side_effect = no_key_error("Not found")

        with patch("flows.daily_digest.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_s3
            result = load_reddit_trending.fn(bucket="test-bucket")

        assert result == {}

    def test_load_trending_s3_error(self):
        """Returns empty dict on S3 errors."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = Exception("Connection timeout")

        with patch("flows.daily_digest.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_s3
            result = load_reddit_trending.fn(bucket="test-bucket")

        assert result == {}


# ============================================================================
# Test Claude Synopsis Generation
# ============================================================================

class TestGenerateClaudeSynopsis:
    """Test Claude AI synopsis generation."""

    def test_synopsis_generated(self, sample_stock_context, digest_settings):
        """Successfully generates synopsis via Claude API."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="NVDA shows strong momentum. BULLISH")]

        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response

        with patch("flows.daily_digest.os.getenv", return_value="sk-test-key"), \
             patch("flows.daily_digest.load_settings", return_value=digest_settings), \
             patch("flows.daily_digest.anthropic") as mock_anthropic:
            mock_anthropic.Anthropic.return_value = mock_client
            result = generate_claude_synopsis.fn("NVDA", sample_stock_context, 25)

        assert result["symbol"] == "NVDA"
        assert "BULLISH" in result["synopsis"]
        assert result["reddit_mentions"] == 25
        assert "error" not in result

    def test_synopsis_no_api_key(self, sample_stock_context, digest_settings):
        """Returns fallback when API key is not set."""
        with patch("flows.daily_digest.os.getenv", return_value=""), \
             patch("flows.daily_digest.load_settings", return_value=digest_settings):
            result = generate_claude_synopsis.fn("NVDA", sample_stock_context, 25)

        assert result["symbol"] == "NVDA"
        assert result["error"] == "no_api_key"
        assert "unavailable" in result["synopsis"]

    def test_synopsis_api_error(self, sample_stock_context, digest_settings):
        """Handles Claude API errors gracefully."""
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("Rate limited")

        with patch("flows.daily_digest.os.getenv", return_value="sk-test-key"), \
             patch("flows.daily_digest.load_settings", return_value=digest_settings), \
             patch("flows.daily_digest.anthropic") as mock_anthropic:
            mock_anthropic.Anthropic.return_value = mock_client
            result = generate_claude_synopsis.fn("NVDA", sample_stock_context, 25)

        assert result["symbol"] == "NVDA"
        assert "error" in result
        assert "Rate limited" in result["error"]

    def test_synopsis_minimal_context(self, digest_settings):
        """Works with minimal stock context (just symbol)."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Limited data. NEUTRAL")]

        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response

        with patch("flows.daily_digest.os.getenv", return_value="sk-test-key"), \
             patch("flows.daily_digest.load_settings", return_value=digest_settings), \
             patch("flows.daily_digest.anthropic") as mock_anthropic:
            mock_anthropic.Anthropic.return_value = mock_client
            result = generate_claude_synopsis.fn("UNKNOWN", {"symbol": "UNKNOWN"}, 0)

        assert result["symbol"] == "UNKNOWN"
        assert "error" not in result


# ============================================================================
# Test Email Sending
# ============================================================================

class TestSendDigestEmail:
    """Test digest email formatting and sending."""

    def test_email_sent(self):
        """Email is sent when properly configured."""
        synopses = [
            {"symbol": "NVDA", "synopsis": "Strong buy. BULLISH", "reddit_mentions": 25},
            {"symbol": "PLTR", "synopsis": "Steady growth. NEUTRAL", "reddit_mentions": 18},
        ]
        settings = {"alerts": {"email_enabled": True}}

        mock_smtp = MagicMock()

        with patch("flows.daily_digest.load_settings", return_value=settings), \
             patch("flows.daily_digest.os.getenv", side_effect=lambda k, d="": {
                 "ALERT_EMAIL_FROM": "test@gmail.com",
                 "ALERT_EMAIL_TO": "user@gmail.com",
                 "GMAIL_APP_PASSWORD": "testpass",
             }.get(k, d)), \
             patch("flows.daily_digest.smtplib.SMTP_SSL") as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            send_digest_email.fn(synopses)

        mock_smtp.send_message.assert_called_once()

    def test_email_skipped_when_disabled(self):
        """Email is not sent when disabled in settings."""
        synopses = [{"symbol": "NVDA", "synopsis": "Test", "reddit_mentions": 5}]
        settings = {"alerts": {"email_enabled": False}}

        with patch("flows.daily_digest.load_settings", return_value=settings):
            send_digest_email.fn(synopses)
        # No exception means it skipped gracefully

    def test_email_skipped_empty_synopses(self):
        """No email sent when synopsis list is empty."""
        send_digest_email.fn([])
        # No exception means it handled gracefully

    def test_email_includes_signals(self):
        """Signal results are included in email when provided."""
        synopses = [
            {"symbol": "NVDA", "synopsis": "Strong. BULLISH", "reddit_mentions": 25},
        ]
        signals = {
            "signals": [{
                "symbol": "NVDA",
                "signal": "Momentum Surge",
                "description": "7-day momentum at 5.2%",
                "price": 875.50,
                "strength": "strong",
            }],
        }
        settings = {"alerts": {"email_enabled": True}}

        mock_smtp = MagicMock()

        with patch("flows.daily_digest.load_settings", return_value=settings), \
             patch("flows.daily_digest.os.getenv", side_effect=lambda k, d="": {
                 "ALERT_EMAIL_FROM": "test@gmail.com",
                 "ALERT_EMAIL_TO": "user@gmail.com",
                 "GMAIL_APP_PASSWORD": "testpass",
             }.get(k, d)), \
             patch("flows.daily_digest.smtplib.SMTP_SSL") as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            send_digest_email.fn(synopses, signal_results=signals)

        mock_smtp.send_message.assert_called_once()


# ============================================================================
# Test Artifact Creation
# ============================================================================

class TestCreateDigestArtifact:
    """Test Prefect artifact creation."""

    def test_artifact_created_with_synopses(self):
        """Artifact is created with synopsis content."""
        synopses = [
            {"symbol": "NVDA", "synopsis": "Strong buy. BULLISH", "reddit_mentions": 25},
        ]

        with patch("flows.daily_digest.create_markdown_artifact") as mock_artifact:
            create_digest_artifact.fn(synopses)

        mock_artifact.assert_called_once()
        call_kwargs = mock_artifact.call_args[1]
        assert "NVDA" in call_kwargs["markdown"]
        assert "BULLISH" in call_kwargs["markdown"]

    def test_artifact_created_empty(self):
        """Artifact is created even with no synopses."""
        with patch("flows.daily_digest.create_markdown_artifact") as mock_artifact:
            create_digest_artifact.fn([])

        mock_artifact.assert_called_once()
        call_kwargs = mock_artifact.call_args[1]
        assert "No stock synopses" in call_kwargs["markdown"]
