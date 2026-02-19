"""Tests for Low Volatility Momentum Strategy."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from signal_service.strategy.low_vol_momentum import LowVolMomentumStrategy


class TestLowVolMomentumStrategy:
    """Test Low Volatility Momentum Strategy."""

    @pytest.fixture
    def strategy(self):
        """Create strategy instance with test-friendly params."""
        return LowVolMomentumStrategy(
            strategy_id="test-123",
            name="low_vol_momentum",
            version="1.0.0",
            symbols=["BTC"],
            timeframes=["15m"],
            params={
                "atr_period": 14,
                "lookback_days": 1,  # shorter for tests (96 bars)
                "low_vol_threshold": 100,  # force low regime
                "momentum_lookback": 6,  # hours
                "stop_loss_pct": 2.0,
                "max_hold_hours": 48,
                "exit_on_regime_change": True,
                "require_candle_confirmation": False,
            },
        )

    @pytest.fixture
    def sample_history(self):
        """Create sample OHLC history with rising trend."""
        np.random.seed(42)
        n = 120  # > 96 bars needed for 1-day lookback
        base_price = 50000
        # steady upward drift
        prices = base_price * (1 + np.linspace(0, 0.30, n))
        timestamps = [datetime.now(timezone.utc) - timedelta(minutes=15 * (n - i)) for i in range(n)]

        df = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": prices * (1 + np.random.normal(0, 0.0005, n)),
                "high": prices * (1 + abs(np.random.normal(0, 0.001, n))),
                "low": prices * (1 - abs(np.random.normal(0, 0.001, n))),
                "close": prices,
                "volume": np.random.uniform(100, 1000, n),
            }
        )
        return df

    def test_strategy_creation(self, strategy):
        assert strategy.name == "low_vol_momentum"
        assert strategy.atr_period == 14
        assert strategy.low_vol_threshold == 100
        assert strategy.momentum_lookback == 6
        assert strategy.max_hold_hours == 48
        assert strategy.exit_on_regime_change is True

    def test_entry_without_candle_confirmation(self, strategy, sample_history):
        """Should enter even if candle is bearish when confirmation is disabled."""
        last_price = sample_history["close"].iloc[-1]
        candle = {
            "timestamp": datetime.now(timezone.utc),
            "symbol": "BTC",
            "open": last_price * 1.01,  # bearish candle
            "high": last_price * 1.02,
            "low": last_price * 0.99,
            "close": last_price * 0.99,
            "volume": 1000,
        }

        signal = strategy.on_candle(candle, sample_history)
        assert signal is not None
        assert signal.side == "long"

    def test_exit_on_stop_loss(self, strategy):
        """Exit when stop loss breached."""
        now = datetime.now(timezone.utc)
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_price": 100.0,
            "entry_ts": now - timedelta(hours=1),
            "entry_regime": "low",
        }
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": 99.0,
            "high": 100.0,
            "low": 96.0,
            "close": 97.0,  # -3% from entry
            "volume": 1000,
        }
        history = pd.DataFrame(
            {
                "timestamp": [now - timedelta(minutes=15)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.0],
                "volume": [1000],
            }
        )

        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "short"
        assert signal.meta.get("exit_reason") == "stop_loss"

    def test_exit_on_max_hold(self, strategy):
        """Exit when max hold time exceeded."""
        now = datetime.now(timezone.utc)
        strategy.max_hold_hours = 0
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_price": 100.0,
            "entry_ts": now - timedelta(hours=1),
            "entry_regime": "low",
        }
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1000,
        }
        history = pd.DataFrame(
            {
                "timestamp": [now - timedelta(minutes=15)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.0],
                "volume": [1000],
            }
        )

        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "short"
        assert signal.meta.get("exit_reason") == "max_hold"
