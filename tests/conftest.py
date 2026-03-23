"""Shared fixtures for market data pipeline tests."""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch


@pytest.fixture
def sample_ml_features_df():
    """DataFrame mimicking ML features output from Spark jobs."""
    now = datetime.now()
    return pd.DataFrame([
        # AAPL - two data points for cross detection
        {
            "symbol": "AAPL",
            "timestamp": pd.Timestamp("2025-01-14"),
            "price": 180.0,
            "ma_7d": 175.0,
            "ma_30d": 178.0,
            "volatility_7d": 1.5,
            "price_momentum_7d": 2.0,
        },
        {
            "symbol": "AAPL",
            "timestamp": pd.Timestamp("2025-01-15"),
            "price": 185.0,
            "ma_7d": 182.0,
            "ma_30d": 179.0,
            "volatility_7d": 1.2,
            "price_momentum_7d": 4.0,
        },
        # MSFT - two data points, no signals
        {
            "symbol": "MSFT",
            "timestamp": pd.Timestamp("2025-01-14"),
            "price": 400.0,
            "ma_7d": 405.0,
            "ma_30d": 402.0,
            "volatility_7d": 3.0,
            "price_momentum_7d": 0.5,
        },
        {
            "symbol": "MSFT",
            "timestamp": pd.Timestamp("2025-01-15"),
            "price": 398.0,
            "ma_7d": 403.0,
            "ma_30d": 404.0,
            "volatility_7d": 3.5,
            "price_momentum_7d": -0.5,
        },
    ])


@pytest.fixture
def all_rules_enabled():
    """Pipeline settings with all buy signal rules enabled."""
    return {
        "buy_signals": {
            "enabled": True,
            "rules": {
                "golden_cross": {"enabled": True},
                "momentum_surge": {"enabled": True, "min_momentum_pct": 3.0},
                "oversold_bounce": {"enabled": True},
                "low_vol_uptrend": {
                    "enabled": True,
                    "max_volatility_7d_pct": 2.0,
                    "min_momentum_pct": 1.0,
                },
            },
        },
        "alerts": {"email_enabled": False},
    }


@pytest.fixture
def all_rules_disabled():
    """Pipeline settings with buy signals disabled."""
    return {
        "buy_signals": {
            "enabled": False,
            "rules": {},
        },
        "alerts": {"email_enabled": False},
    }
