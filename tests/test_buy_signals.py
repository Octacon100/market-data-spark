"""Tests for buy signal detection logic in flows/buy_signal_alerts.py"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock
import io
import json


# We test the signal detection logic by mocking S3 and settings.
# The detect_buy_signals function is a Prefect task, so we call .fn() to skip
# the Prefect runtime wrapper.

from flows.buy_signal_alerts import detect_buy_signals, load_settings


def _mock_s3_with_dataframe(df, bucket="test-bucket"):
    """Helper: mock boto3 S3 client to return a DataFrame as parquet."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    parquet_bytes = buf.getvalue()

    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "analytics/ml_features/part-00000.parquet"}]}
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(parquet_bytes)
    }
    return mock_s3


class TestGoldenCross:
    """Test golden cross signal: 7d MA crosses above 30d MA."""

    def test_golden_cross_detected(self, sample_ml_features_df, all_rules_enabled):
        """AAPL has ma_7d crossing above ma_30d between the two data points."""
        mock_s3 = _mock_s3_with_dataframe(sample_ml_features_df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        golden = [s for s in signals if s["signal"] == "Golden Cross"]
        assert len(golden) == 1
        assert golden[0]["symbol"] == "AAPL"
        assert golden[0]["strength"] == "strong"
        assert golden[0]["ma_7d"] == 182.0
        assert golden[0]["ma_30d"] == 179.0

    def test_golden_cross_not_detected_when_disabled(self, sample_ml_features_df, all_rules_enabled):
        """No golden cross when the rule is disabled."""
        all_rules_enabled["buy_signals"]["rules"]["golden_cross"]["enabled"] = False
        mock_s3 = _mock_s3_with_dataframe(sample_ml_features_df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        golden = [s for s in signals if s["signal"] == "Golden Cross"]
        assert len(golden) == 0

    def test_no_golden_cross_when_ma_already_above(self, all_rules_enabled):
        """No cross when 7d MA was already above 30d MA in prev period."""
        df = pd.DataFrame([
            {"symbol": "TEST", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 100.0, "ma_7d": 105.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 1.0},
            {"symbol": "TEST", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 102.0, "ma_7d": 106.0, "ma_30d": 101.0,
             "volatility_7d": 1.0, "price_momentum_7d": 1.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        golden = [s for s in signals if s["signal"] == "Golden Cross"]
        assert len(golden) == 0


class TestMomentumSurge:
    """Test momentum surge signal: 7d momentum >= threshold."""

    def test_momentum_surge_detected(self, sample_ml_features_df, all_rules_enabled):
        """AAPL has momentum 4.0 >= threshold 3.0."""
        mock_s3 = _mock_s3_with_dataframe(sample_ml_features_df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        momentum = [s for s in signals if s["signal"] == "Momentum Surge"]
        assert len(momentum) == 1
        assert momentum[0]["symbol"] == "AAPL"
        assert momentum[0]["momentum_7d"] == 4.0
        assert momentum[0]["strength"] == "moderate"  # 4.0 < 6.0 (2x threshold)

    def test_strong_momentum_surge(self, all_rules_enabled):
        """Momentum >= 2x threshold should be 'strong'."""
        df = pd.DataFrame([
            {"symbol": "TSLA", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 200.0, "ma_7d": 195.0, "ma_30d": 200.0,
             "volatility_7d": 3.0, "price_momentum_7d": 1.0},
            {"symbol": "TSLA", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 220.0, "ma_7d": 210.0, "ma_30d": 205.0,
             "volatility_7d": 3.0, "price_momentum_7d": 7.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        momentum = [s for s in signals if s["signal"] == "Momentum Surge"]
        assert len(momentum) == 1
        assert momentum[0]["strength"] == "strong"

    def test_no_momentum_below_threshold(self, all_rules_enabled):
        """Momentum below threshold should not trigger."""
        df = pd.DataFrame([
            {"symbol": "SLOW", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 50.0, "ma_7d": 49.0, "ma_30d": 50.0,
             "volatility_7d": 1.0, "price_momentum_7d": 0.5},
            {"symbol": "SLOW", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 51.0, "ma_7d": 50.0, "ma_30d": 50.0,
             "volatility_7d": 1.0, "price_momentum_7d": 2.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        momentum = [s for s in signals if s["signal"] == "Momentum Surge"]
        assert len(momentum) == 0


class TestOversoldBounce:
    """Test oversold bounce signal: price crosses above 30d MA."""

    def test_oversold_bounce_detected(self, all_rules_enabled):
        """Price goes from below to above 30d MA."""
        df = pd.DataFrame([
            {"symbol": "BOUNCE", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 95.0, "ma_7d": 96.0, "ma_30d": 100.0,
             "volatility_7d": 2.0, "price_momentum_7d": -1.0},
            {"symbol": "BOUNCE", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 102.0, "ma_7d": 98.0, "ma_30d": 100.0,
             "volatility_7d": 2.0, "price_momentum_7d": 5.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        bounce = [s for s in signals if s["signal"] == "Oversold Bounce"]
        assert len(bounce) == 1
        assert bounce[0]["symbol"] == "BOUNCE"
        assert bounce[0]["strength"] == "moderate"

    def test_no_bounce_when_price_stays_above(self, all_rules_enabled):
        """No bounce if price was already above 30d MA."""
        df = pd.DataFrame([
            {"symbol": "STABLE", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 105.0, "ma_7d": 103.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 1.0},
            {"symbol": "STABLE", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 107.0, "ma_7d": 104.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 1.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        bounce = [s for s in signals if s["signal"] == "Oversold Bounce"]
        assert len(bounce) == 0


class TestLowVolUptrend:
    """Test low volatility uptrend: low vol + positive momentum."""

    def test_low_vol_uptrend_detected(self, sample_ml_features_df, all_rules_enabled):
        """AAPL has vol=1.2 < 2.0 and momentum=4.0 >= 1.0."""
        mock_s3 = _mock_s3_with_dataframe(sample_ml_features_df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        low_vol = [s for s in signals if s["signal"] == "Low Vol Uptrend"]
        assert len(low_vol) == 1
        assert low_vol[0]["symbol"] == "AAPL"
        assert low_vol[0]["volatility_7d"] == 1.2

    def test_no_low_vol_when_volatility_high(self, all_rules_enabled):
        """High volatility should not trigger."""
        df = pd.DataFrame([
            {"symbol": "WILD", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 100.0, "ma_7d": 99.0, "ma_30d": 98.0,
             "volatility_7d": 5.0, "price_momentum_7d": 2.0},
            {"symbol": "WILD", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 105.0, "ma_7d": 101.0, "ma_30d": 99.0,
             "volatility_7d": 4.5, "price_momentum_7d": 3.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        low_vol = [s for s in signals if s["signal"] == "Low Vol Uptrend"]
        assert len(low_vol) == 0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_buy_signals_disabled_returns_empty(self, all_rules_disabled):
        """When buy_signals.enabled is False, return empty list immediately."""
        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_disabled):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        assert signals == []

    def test_empty_s3_returns_empty(self, all_rules_enabled):
        """No parquet files in S3 returns empty list."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        assert signals == []

    def test_single_data_point_skipped(self, all_rules_enabled):
        """Symbols with only one data point are skipped (need 2 for cross detection)."""
        df = pd.DataFrame([
            {"symbol": "LONE", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 100.0, "ma_7d": 105.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 5.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        assert signals == []

    def test_s3_error_returns_empty(self, all_rules_enabled):
        """S3 errors are caught and return empty list."""
        mock_s3 = MagicMock()
        mock_s3.get_paginator.side_effect = Exception("S3 connection failed")

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        assert signals == []

    def test_multiple_signals_for_same_symbol(self, all_rules_enabled):
        """A symbol can trigger multiple signals at once."""
        # AAPL in sample data triggers golden_cross, momentum_surge, AND low_vol_uptrend
        df = pd.DataFrame([
            {"symbol": "HOT", "timestamp": pd.Timestamp("2025-01-14"),
             "price": 95.0, "ma_7d": 96.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 1.0},
            {"symbol": "HOT", "timestamp": pd.Timestamp("2025-01-15"),
             "price": 105.0, "ma_7d": 103.0, "ma_30d": 100.0,
             "volatility_7d": 1.0, "price_momentum_7d": 5.0},
        ])
        mock_s3 = _mock_s3_with_dataframe(df)

        with patch("flows.buy_signal_alerts.load_settings", return_value=all_rules_enabled), \
             patch("flows.buy_signal_alerts.make_boto3_client", return_value=mock_s3):
            signals = detect_buy_signals.fn(bucket="test-bucket")

        # Should get: golden_cross, momentum_surge, oversold_bounce, low_vol_uptrend
        signal_types = {s["signal"] for s in signals}
        assert "Golden Cross" in signal_types
        assert "Momentum Surge" in signal_types
        assert "Oversold Bounce" in signal_types
        assert "Low Vol Uptrend" in signal_types


class TestLoadSettings:
    """Test settings loading."""

    def test_load_settings_missing_file(self, tmp_path):
        """Returns empty dict when file doesn't exist."""
        with patch("flows.buy_signal_alerts.SETTINGS_PATH", tmp_path / "nonexistent.json"):
            result = load_settings()
        assert result == {}

    def test_load_settings_valid_file(self, tmp_path):
        """Loads JSON correctly from file."""
        settings_file = tmp_path / "settings.json"
        settings_file.write_text('{"buy_signals": {"enabled": true}}')
        with patch("flows.buy_signal_alerts.SETTINGS_PATH", settings_file):
            result = load_settings()
        assert result["buy_signals"]["enabled"] is True
