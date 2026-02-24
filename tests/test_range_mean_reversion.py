"""Tests for RangeMeanReversionStrategy with exit logic."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from signal_service.strategy.range_mean_reversion import RangeMeanReversionStrategy


class TestRangeMeanReversionExits:
    """Test exit logic for RangeMeanReversionStrategy."""

    def create_history(self, n_bars=50, base_price=100.0):
        """Create sample OHLCV history."""
        timestamps = pd.date_range(
            start="2026-02-24 09:00:00",
            periods=n_bars,
            freq="5min",
            tz="UTC"
        )

        data = {
            "timestamp": timestamps,
            "open": [base_price] * n_bars,
            "high": [base_price * 1.01] * n_bars,
            "low": [base_price * 0.99] * n_bars,
            "close": [base_price] * n_bars,
            "volume": [1000.0] * n_bars,
        }
        return pd.DataFrame(data)

    def create_candle(self, close, timestamp=None, base_price=100.0):
        """Create a sample candle."""
        if timestamp is None:
            timestamp = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)
        return {
            "timestamp": timestamp,
            "open": base_price,
            "high": base_price * 1.01,
            "low": base_price * 0.99,
            "close": close,
            "volume": 1000.0,
        }

    def test_exit_logic_runs_with_limited_history(self):
        """Exit logic should run even when history < min_bars for entry."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
            "stop_loss_enabled": True,
            "stop_loss_multiplier": 1.5,
        })

        # Create minimal history (less than min_bars for entry)
        history = self.create_history(n_bars=10, base_price=100.0)

        # Simulate entry
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)
        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # Price returns to VWAP - should exit even with limited history
        candle = self.create_candle(close=99.9, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "vwap_mean_reversion"

    def test_vwap_mean_reversion_exit_long(self):
        """Test VWAP mean reversion exit for long position."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        # Enter long below VWAP
        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # Price rises to VWAP (99.9 >= 100 * 0.998 = 99.8) - should exit
        candle = self.create_candle(close=99.9, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "vwap_mean_reversion"

    def test_vwap_mean_reversion_exit_short(self):
        """Test VWAP mean reversion exit for short position."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        # Enter short above VWAP
        strategy._set_position("short", 102.0, 100.0, 2.0, entry_ts)

        # Price falls to VWAP (100.1 <= 100 * 1.002 = 100.2) - should exit
        candle = self.create_candle(close=100.1, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "vwap_mean_reversion"

    def test_no_vwap_exit_when_price_not_at_vwap(self):
        """Should not exit if price hasn't returned to VWAP."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # Price at 99.0 (below threshold 99.8) - should NOT exit via VWAP
        # But within max_hold time - no exit at all
        candle = self.create_candle(close=99.0, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is None  # No exit

    def test_timestamp_based_max_hold_exit(self):
        """Test time-based exit using timestamp (not bar index)."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 30,  # 30 minutes max hold
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # After 31 minutes - should exit via time
        exit_ts = entry_ts + timedelta(minutes=31)
        candle = self.create_candle(close=99.0, timestamp=exit_ts)
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "max_hold_time"
        assert signal.meta.get("hold_minutes") == 31

    def test_no_time_exit_within_max_hold(self):
        """Should not time-exit if within max_hold_minutes."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # After 30 minutes - within 75 min max hold
        exit_ts = entry_ts + timedelta(minutes=30)
        candle = self.create_candle(close=99.0, timestamp=exit_ts)
        signal = strategy.on_candle(candle, history)

        assert signal is None  # No exit

    def test_stop_loss_exit_long(self):
        """Test stop loss exit for long position."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
            "stop_loss_enabled": True,
            "stop_loss_multiplier": 1.5,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        # Long entered at 2% below VWAP
        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # Stop at 1.5x deviation = 3% below VWAP = 97.0
        # Price at 96.5 - should trigger stop
        candle = self.create_candle(close=96.5, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "stop_loss"

    def test_stop_loss_exit_short(self):
        """Test stop loss exit for short position."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
            "stop_loss_enabled": True,
            "stop_loss_multiplier": 1.5,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        # Short entered at 2% above VWAP
        strategy._set_position("short", 102.0, 100.0, 2.0, entry_ts)

        # Stop at 1.5x deviation = 3% above VWAP = 103.0
        # Price at 103.5 - should trigger stop
        candle = self.create_candle(close=103.5, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "stop_loss"

    def test_no_stop_loss_when_disabled(self):
        """Should not stop out if stop_loss is disabled."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
            "max_hold_minutes": 75,
            "stop_loss_enabled": False,
            "stop_loss_multiplier": 1.5,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)

        # Price way below stop threshold
        candle = self.create_candle(close=90.0, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is None  # No stop loss exit

    def test_entry_requires_full_history(self):
        """Entry should require min_bars of history."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_lookback": 20,
            "rsi_period": 14,
            "ema_filter_period": 50,
        })

        # Not enough history for entry (need 50+5=55 bars)
        history = self.create_history(n_bars=30, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        # No position - try to enter with insufficient history
        candle = self.create_candle(close=98.0, timestamp=entry_ts)
        signal = strategy.on_candle(candle, history)

        assert signal is None  # No entry

    def test_position_reset_after_exit(self):
        """Position state should reset after exit."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_tolerance": 0.002,
        })

        history = self.create_history(n_bars=50, base_price=100.0)
        entry_ts = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)

        strategy._set_position("long", 98.0, 100.0, -2.0, entry_ts)
        assert strategy._position is not None

        # Exit
        candle = self.create_candle(close=99.9, timestamp=entry_ts + timedelta(minutes=10))
        signal = strategy.on_candle(candle, history)

        assert signal is not None
        assert strategy._position is None  # Reset after exit


class TestRangeMeanReversionEntry:
    """Test entry logic for RangeMeanReversionStrategy."""

    def create_history(self, n_bars=60, trend="flat"):
        """Create sample OHLCV history with optional trend."""
        timestamps = pd.date_range(
            start="2026-02-24 09:00:00",
            periods=n_bars,
            freq="5min",
            tz="UTC"
        )

        base_price = 100.0
        closes = []
        for i in range(n_bars):
            if trend == "flat":
                closes.append(base_price)
            elif trend == "up":
                closes.append(base_price + i * 0.1)
            elif trend == "down":
                closes.append(base_price - i * 0.1)

        data = {
            "timestamp": timestamps,
            "open": [c * 0.99 for c in closes],
            "high": [c * 1.01 for c in closes],
            "low": [c * 0.99 for c in closes],
            "close": closes,
            "volume": [1000.0] * n_bars,
        }
        return pd.DataFrame(data)

    def create_oversold_candle(self, close, timestamp=None):
        """Create a candle that looks oversold (for long entry)."""
        if timestamp is None:
            timestamp = datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc)
        return {
            "timestamp": timestamp,
            "open": close * 1.01,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "volume": 5000.0,  # Higher volume
        }

    def test_long_entry_conditions(self):
        """Test long entry with oversold conditions."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_lookback": 20,
            "rsi_period": 14,
            "rsi_oversold": 30,
            "deviation_pct": 1.0,
        })

        # Flat trend history
        history = self.create_history(n_bars=60, trend="flat")

        # Oversold candle far below VWAP
        candle = self.create_oversold_candle(close=98.0)
        signal = strategy.on_candle(candle, history)

        # Should enter long
        assert signal is not None
        assert signal.side == "long"
        assert "vwap" in signal.meta
        assert "deviation_pct" in signal.meta

    def test_no_entry_in_strong_trend(self):
        """Should not enter when trend is not flat."""
        strategy = RangeMeanReversionStrategy(params={
            "ema_filter_period": 50,
        })

        # Strong uptrend - EMA slope will be steep
        history = self.create_history(n_bars=60, trend="up")

        candle = {
            "timestamp": datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc),
            "open": 98.0,
            "high": 99.0,
            "low": 97.0,
            "close": 98.0,
            "volume": 1000.0,
        }
        signal = strategy.on_candle(candle, history)

        # Should not enter due to trend filter
        assert signal is None

    def test_short_entry_conditions(self):
        """Test short entry with overbought conditions."""
        strategy = RangeMeanReversionStrategy(params={
            "vwap_lookback": 20,
            "rsi_period": 14,
            "rsi_overbought": 70,
            "deviation_pct": 1.0,
        })

        history = self.create_history(n_bars=60, trend="flat")

        # Overbought candle far above VWAP
        candle = {
            "timestamp": datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc),
            "open": 102.0,
            "high": 103.0,
            "low": 101.0,
            "close": 102.0,
            "volume": 5000.0,
        }
        signal = strategy.on_candle(candle, history)

        # Should enter short
        assert signal is not None
        assert signal.side == "short"

    def test_atr_filter_blocks_entry(self):
        """High ATR should block entry."""
        strategy = RangeMeanReversionStrategy(params={
            "max_atr_pct": 0.5,  # Very strict ATR filter
        })

        history = self.create_history(n_bars=60, trend="flat")
        # Make history more volatile
        history["high"] = history["close"] * 1.05
        history["low"] = history["close"] * 0.95

        candle = self.create_oversold_candle(close=98.0)
        signal = strategy.on_candle(candle, history)

        # Should not enter due to high ATR
        assert signal is None
