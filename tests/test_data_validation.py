"""Tests for data validation logic in flows/market_data_flow.py"""

import pytest
import sys
from unittest.mock import patch, MagicMock
from dataclasses import dataclass


def _make_stock_data(**overrides):
    """Create a StockData instance with sensible defaults."""
    from flows.market_data_flow import StockData
    defaults = {
        "symbol": "TEST",
        "timestamp": "2025-01-15T10:30:00",
        "price": 150.0,
        "volume": 1000000,
        "open": 148.0,
        "high": 152.0,
        "low": 147.0,
        "previous_close": 149.0,
        "change": 1.0,
        "change_percent": "0.67%",
    }
    defaults.update(overrides)
    return StockData(**defaults)


@pytest.fixture(autouse=True)
def mock_pipeline_config():
    """Patch resolve and make_boto3_client before market_data_flow imports them."""
    # If the module hasn't been imported yet, patch config_utils before it loads
    mock_resolve = lambda name, env, **kw: {
        'alpha-vantage-api-key': 'test-key',
        's3-bucket': 'test-bucket',
        'aws-access-key-id': '',
        'aws-secret-access-key': '',
        'aws-region': 'us-east-1',
    }.get(name, '')

    with patch("config_utils.resolve", side_effect=mock_resolve), \
         patch("config_utils.make_boto3_client", return_value=MagicMock()):

        # Force re-import if already cached with bad config
        for mod_name in list(sys.modules.keys()):
            if 'market_data_flow' in mod_name:
                del sys.modules[mod_name]

        import flows.market_data_flow as mdflow

        mock_config = MagicMock()
        mock_config.alpha_vantage_key = "test-key"
        mock_config.s3_bucket = "test-bucket"
        mock_config.symbols = ["TEST"]
        mock_config.pipeline_version = "1.0.0"

        original_config = mdflow.config
        original_s3 = mdflow.s3
        mdflow.config = mock_config
        mdflow.s3 = MagicMock()
        yield mock_config
        mdflow.config = original_config
        mdflow.s3 = original_s3


class TestPriceValidation:
    """Test that invalid prices are rejected."""

    def test_valid_price_passes(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=150.0)
        result = validate_data.fn(data)
        assert result.price == 150.0

    def test_zero_price_rejected(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=0)
        with pytest.raises(ValueError, match="Invalid price"):
            validate_data.fn(data)

    def test_negative_price_rejected(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=-10.0)
        with pytest.raises(ValueError, match="Invalid price"):
            validate_data.fn(data)

    @patch("flows.market_data_flow.emit_event")
    def test_penny_stock_price_valid(self, mock_emit):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=0.01)
        result = validate_data.fn(data)
        assert result.price == 0.01


class TestVolumeValidation:
    """Test that invalid volumes are rejected."""

    def test_valid_volume_passes(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(volume=1000000)
        result = validate_data.fn(data)
        assert result.volume == 1000000

    def test_zero_volume_passes(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(volume=0)
        result = validate_data.fn(data)
        assert result.volume == 0

    def test_negative_volume_rejected(self):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(volume=-100)
        with pytest.raises(ValueError, match="Invalid volume"):
            validate_data.fn(data)


class TestAnomalyDetection:
    """Test large price movement detection."""

    @patch("flows.market_data_flow.emit_event")
    def test_normal_movement_no_anomaly(self, mock_emit):
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=150.0, previous_close=149.0)
        validate_data.fn(data)
        mock_emit.assert_not_called()

    @patch("flows.market_data_flow.emit_event")
    def test_large_movement_triggers_medium_anomaly(self, mock_emit):
        """Movement >20% but <=30% should emit medium severity."""
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=125.0, previous_close=100.0)  # 25% move
        validate_data.fn(data)
        mock_emit.assert_called_once()
        payload = mock_emit.call_args[1]["payload"]
        assert payload["severity"] == "medium"
        assert payload["change_percent"] == 25.0

    @patch("flows.market_data_flow.emit_event")
    def test_extreme_movement_triggers_high_anomaly(self, mock_emit):
        """Movement >30% should emit high severity."""
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=140.0, previous_close=100.0)  # 40% move
        validate_data.fn(data)
        mock_emit.assert_called_once()
        payload = mock_emit.call_args[1]["payload"]
        assert payload["severity"] == "high"

    @patch("flows.market_data_flow.emit_event")
    def test_large_drop_triggers_anomaly(self, mock_emit):
        """Large drops (negative) should also trigger anomaly."""
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=75.0, previous_close=100.0)  # -25% move
        validate_data.fn(data)
        mock_emit.assert_called_once()
        payload = mock_emit.call_args[1]["payload"]
        assert payload["severity"] == "medium"

    @patch("flows.market_data_flow.emit_event")
    def test_no_previous_close_skips_anomaly(self, mock_emit):
        """No anomaly check when previous_close is 0."""
        from flows.market_data_flow import validate_data
        data = _make_stock_data(price=100.0, previous_close=0)
        validate_data.fn(data)
        mock_emit.assert_not_called()


class TestStockDataModel:
    """Test StockData dataclass."""

    def test_to_dict(self):
        data = _make_stock_data()
        d = data.to_dict()
        assert d["symbol"] == "TEST"
        assert d["price"] == 150.0
        assert d["source"] == "alphavantage"

    def test_default_source(self):
        data = _make_stock_data()
        assert data.source == "alphavantage"


class TestWatchlistLoader:
    """Test watchlist.json loading."""

    def test_load_watchlist_returns_list(self, tmp_path):
        from flows.market_data_flow import load_watchlist
        watchlist_file = tmp_path / "watchlist.json"
        watchlist_file.write_text('["AAPL", "MSFT", "GOOGL"]')
        with patch("flows.market_data_flow.WATCHLIST_PATH", watchlist_file):
            result = load_watchlist()
        assert result == ["AAPL", "MSFT", "GOOGL"]

    def test_load_watchlist_missing_file(self, tmp_path):
        from flows.market_data_flow import load_watchlist
        with patch("flows.market_data_flow.WATCHLIST_PATH", tmp_path / "missing.json"):
            result = load_watchlist()
        assert result == []
