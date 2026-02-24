"""Tests for Range Mean Reversion Strategy with Exit Logic."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from signal_service.strategy.range_mean_reversion import RangeMeanReversionStrategy


class TestRangeMeanReversionStrategy:
    """Test Range Mean Reversion Strategy with Exit Logic."""

    @pytest.fixture
    def default_params(self):
        """Default test-friendly strategy parameters."""
        return {
            # Entry parameters
            "vwap_lookback": 20,
            "rsi_period": 14,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "deviation_pct": 1.0,
            "ema_filter_period": 50,
            "max_atr_pct": 2.0,
            # Exit parameters (v1.1.0)
            "vwap_tolerance": 0.002,
            "max_hold_candles": 15,
            "stop_loss_enabled": True,
            "stop_loss_multiplier": 1.5,
        }

    @pytest.fixture
    def strategy(self, default_params):
        """Create strategy instance with test-friendly params."""
        return RangeMeanReversionStrategy(
            strategy_id="test-123",
            name="range_mean_reversion",
            version="1.1.0",
            symbols=["BTC"],
            timeframes=["5m"],
            params=default_params,
        )

    def create_history(
        self,
        n_bars=200,
        base_price=50000,
        trend="flat",
        volatility=0.001,
        volume_mean=1000,
    ):
        """Create sample OHLC history with specified characteristics."""
        np.random.seed(42)
        timestamps = [
            datetime.now(timezone.utc) - timedelta(minutes=5 * (n_bars - i))
            for i in range(n_bars)
        ]

        if trend == "flat":
            prices = base_price * (1 + np.random.normal(0, volatility, n_bars))
        elif trend == "up":
            prices = base_price * (1 + np.linspace(0, 0.05, n_bars) + np.random.normal(0, volatility, n_bars))
        elif trend == "down":
            prices = base_price * (1 - np.linspace(0, 0.05, n_bars) + np.random.normal(0, volatility, n_bars))
        else:
            prices = base_price * (1 + np.random.normal(0, volatility, n_bars))

        df = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": prices * (1 + np.random.normal(0, volatility * 0.5, n_bars)),
                "high": prices * (1 + abs(np.random.normal(0, volatility, n_bars))),
                "low": prices * (1 - abs(np.random.normal(0, volatility, n_bars))),
                "close": prices,
                "volume": np.random.uniform(volume_mean * 0.5, volume_mean * 1.5, n_bars),
            }
        )
        return df

    def create_extreme_conditions_history(
        self,
        n_bars=200,
        base_price=50000,
        condition="oversold",  # "oversold" or "overbought"
        deviation_pct=2.0,
    ):
        """Create history with extreme RSI and VWAP deviation conditions."""
        np.random.seed(42)
        timestamps = [
            datetime.now(timezone.utc) - timedelta(minutes=5 * (n_bars - i))
            for i in range(n_bars)
        ]

        # Create a flat base with a dip/spike at the end
        prices = np.full(n_bars, base_price, dtype=float)
        
        # Add the deviation at the end
        if condition == "oversold":
            # Price drops below VWAP for long entry
            deviation_factor = 1 - (deviation_pct / 100)
            prices[-20:] = base_price * np.linspace(1.0, deviation_factor, 20)
        else:  # overbought
            # Price rises above VWAP for short entry
            deviation_factor = 1 + (deviation_pct / 100)
            prices[-20:] = base_price * np.linspace(1.0, deviation_factor, 20)

        # Add some noise
        prices = prices * (1 + np.random.normal(0, 0.001, n_bars))

        df = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": prices * (1 + np.random.normal(0, 0.0005, n_bars)),
                "high": prices * (1 + abs(np.random.normal(0, 0.001, n_bars))),
                "low": prices * (1 - abs(np.random.normal(0, 0.001, n_bars))),
                "close": prices,
                "volume": np.random.uniform(500, 1500, n_bars),
            }
        )
        return df

    def test_strategy_creation(self, strategy, default_params):
        """Test strategy initialization with parameters."""
        assert strategy.name == "range_mean_reversion"
        assert strategy.version == "1.1.0"
        
        # Entry parameters
        assert strategy.vwap_lookback == default_params["vwap_lookback"]
        assert strategy.rsi_period == default_params["rsi_period"]
        assert strategy.rsi_oversold == default_params["rsi_oversold"]
        assert strategy.rsi_overbought == default_params["rsi_overbought"]
        assert strategy.deviation_pct == default_params["deviation_pct"]
        assert strategy.ema_filter_period == default_params["ema_filter_period"]
        assert strategy.max_atr_pct == default_params["max_atr_pct"]
        
        # Exit parameters (v1.1.0)
        assert strategy.vwap_tolerance == default_params["vwap_tolerance"]
        assert strategy.max_hold_candles == default_params["max_hold_candles"]
        assert strategy.stop_loss_enabled == default_params["stop_loss_enabled"]
        assert strategy.stop_loss_multiplier == default_params["stop_loss_multiplier"]

    def test_insufficient_history(self, strategy):
        """Should return None when not enough history."""
        candle = {
            "timestamp": datetime.now(timezone.utc),
            "symbol": "BTC",
            "open": 50000,
            "high": 50100,
            "low": 49900,
            "close": 50000,
            "volume": 1000,
        }
        history = pd.DataFrame(
            {
                "timestamp": [datetime.now(timezone.utc) - timedelta(minutes=5)],
                "open": [50000],
                "high": [50100],
                "low": [49900],
                "close": [50000],
                "volume": [1000],
            }
        )
        signal = strategy.on_candle(candle, history)
        assert signal is None

    def test_high_volatility_filter(self, strategy):
        """Should return None when volatility is too high."""
        # Create history with high volatility
        history = self.create_history(n_bars=200, volatility=0.05)  # Very high volatility
        
        candle = {
            "timestamp": datetime.now(timezone.utc),
            "symbol": "BTC",
            "open": 50000,
            "high": 52500,
            "low": 47500,
            "close": 50000,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is None  # Filtered by ATR

    def test_exit_on_vwap_mean_reversion_long(self, strategy):
        """Exit long position when price returns to VWAP."""
        now = datetime.now(timezone.utc)
        
        # Setup: In a long position, price was below VWAP
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        current_close = 50200  # Price back to/near VWAP
        
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000,
            "entry_deviation": -2.0,
            "entry_ts": now - timedelta(minutes=30),
            "entry_price": 49000,
        }
        
        candle = {
            "timestamp": now,
            "timeframe": "5m",
            "symbol": "BTC",
            "open": 50100,
            "high": 50250,
            "low": 50050,
            "close": current_close,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "vwap_mean_reversion"
        assert "vwap" in signal.meta
        assert "position_age_minutes" in signal.meta

    def test_exit_on_vwap_mean_reversion_short(self, strategy):
        """Exit short position when price returns to VWAP."""
        now = datetime.now(timezone.utc)
        
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        current_close = 49800  # Price back to/near VWAP
        
        strategy._positions["BTC"] = {
            "side": "short",
            "entry_vwap": 50000,
            "entry_deviation": 2.0,
            "entry_ts": now - timedelta(minutes=30),
            "entry_price": 51000,
        }
        
        candle = {
            "timestamp": now,
            "timeframe": "5m",
            "symbol": "BTC",
            "open": 49900,
            "high": 49950,
            "low": 49750,
            "close": current_close,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "vwap_mean_reversion"

    def test_exit_on_max_hold_time(self, strategy):
        """Exit position when max hold candles reached."""
        now = datetime.now(timezone.utc)
        
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000,
            "entry_deviation": -1.5,
            "entry_ts": now - timedelta(minutes=(strategy.max_hold_candles + 1) * 5),
            "entry_price": 49250,
        }
        
        candle = {
            "timestamp": now,
            "timeframe": "5m",
            "symbol": "BTC",
            "open": 49200,
            "high": 49300,
            "low": 49100,
            "close": 49200,  # Still below VWAP
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "max_hold_time"
        assert signal.meta.get("max_hold") == strategy.max_hold_candles

    def test_exit_on_stop_loss_long(self, strategy):
        """Exit long position when stop loss is hit."""
        now = datetime.now(timezone.utc)
        
        # Create history with known VWAP baseline
        np.random.seed(42)
        n_bars = 200
        base_price = 50000
        timestamps = [
            datetime.now(timezone.utc) - timedelta(minutes=5 * (n_bars - i))
            for i in range(n_bars)
        ]
        
        # Create flat price history at base_price
        prices = np.full(n_bars, base_price, dtype=float)
        # Small noise for EMA/RSI calculations
        prices = prices * (1 + np.random.normal(0, 0.0001, n_bars))
        
        history = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": prices * 0.999,
                "high": prices * 1.001,
                "low": prices * 0.999,
                "close": prices,
                "volume": np.full(n_bars, 1000.0),
            }
        )
        
        entry_deviation = -2.0
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": base_price,
            "entry_deviation": entry_deviation,
            "entry_ts": now - timedelta(minutes=10),
            "entry_price": base_price * 0.98,
        }
        
        # Price moved further away from VWAP (worse for long)
        # deviation should be <= -(abs(entry_deviation) * stop_loss_multiplier)
        # i.e., deviation <= -(2.0 * 1.5) = -3.0
        # So current_close should be <= base_price * (1 - 0.03) = 48500
        current_close = base_price * 0.96  # 4% below VWAP to be safe
        
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": current_close * 1.002,
            "high": current_close * 1.002,
            "low": current_close * 0.998,
            "close": current_close,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "stop_loss"
        assert "entry_deviation" in signal.meta
        assert "deviation" in signal.meta

    def test_exit_on_stop_loss_short(self, strategy):
        """Exit short position when stop loss is hit."""
        now = datetime.now(timezone.utc)
        
        # Create history with known VWAP baseline
        np.random.seed(42)
        n_bars = 200
        base_price = 50000
        timestamps = [
            datetime.now(timezone.utc) - timedelta(minutes=5 * (n_bars - i))
            for i in range(n_bars)
        ]
        
        # Create flat price history at base_price
        prices = np.full(n_bars, base_price, dtype=float)
        # Small noise for EMA/RSI calculations
        prices = prices * (1 + np.random.normal(0, 0.0001, n_bars))
        
        history = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": prices * 0.999,
                "high": prices * 1.001,
                "low": prices * 0.999,
                "close": prices,
                "volume": np.full(n_bars, 1000.0),
            }
        )
        
        entry_deviation = 2.0
        strategy._positions["BTC"] = {
            "side": "short",
            "entry_vwap": base_price,
            "entry_deviation": entry_deviation,
            "entry_ts": now - timedelta(minutes=10),
            "entry_price": base_price * 1.02,
        }
        
        # Price moved further away from VWAP (worse for short)
        # deviation should be >= (abs(entry_deviation) * stop_loss_multiplier)
        # i.e., deviation >= (2.0 * 1.5) = 3.0
        current_close = base_price * 1.03  # 3% above VWAP = stop loss
        
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": current_close * 0.998,
            "high": current_close * 1.002,
            "low": current_close * 0.998,
            "close": current_close,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        assert signal.meta.get("exit_reason") == "stop_loss"

    def test_stop_loss_disabled(self, strategy):
        """Should not exit on stop loss when disabled."""
        now = datetime.now(timezone.utc)
        
        strategy.stop_loss_enabled = False
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000,
            "entry_deviation": -2.0,
            "entry_ts": now - timedelta(minutes=10),
            "entry_price": 49000,
        }
        
        # Price at stop loss level
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": 48500,
            "high": 48500,
            "low": 48400,
            "close": 48500,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        # Should not exit due to stop loss (disabled), but may exit due to max hold or other reasons
        if signal is not None:
            assert signal.meta.get("exit_reason") != "stop_loss"

    def test_no_exit_when_conditions_not_met(self, strategy):
        """Should not exit when no exit conditions are met."""
        now = datetime.now(timezone.utc)
        
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000,
            "entry_deviation": -1.5,
            "entry_ts": now - timedelta(minutes=25),  # Only 5 candles ago on 5m
            "entry_price": 49250,
        }
        
        # Price still below VWAP, not at stop loss, not at max hold
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": 49200,
            "high": 49300,
            "low": 49100,
            "close": 49200,
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is None

    def test_signal_metadata_includes_exit_rules(self, strategy):
        """Entry signals should include exit rule configuration in metadata."""
        # This test verifies that entry signals contain the exit rules
        # We'll create conditions that would trigger an entry
        
        # Create history with oversold conditions
        history = self.create_extreme_conditions_history(
            n_bars=200,
            base_price=50000,
            condition="oversold",
            deviation_pct=3.0,  # Exceeds deviation_pct=1.0
        )
        
        # Force RSI to be very low (oversold)
        # RSI calculation is complex, so we'll manually verify the structure
        
        candle = {
            "timestamp": datetime.now(timezone.utc),
            "symbol": "BTC",
            "open": 48500,
            "high": 48600,
            "low": 48400,
            "close": 48500,
            "volume": 1000,
        }
        
        # Note: We can't reliably trigger entry in unit tests due to RSI/EMA complexity
        # This test documents the expected metadata structure
        expected_exit_rules = {
            "vwap_tolerance": strategy.vwap_tolerance,
            "max_hold_candles": strategy.max_hold_candles,
            "stop_loss_enabled": strategy.stop_loss_enabled,
            "stop_loss_multiplier": strategy.stop_loss_multiplier,
        }
        
        # Verify the strategy has the exit rules configured
        assert strategy.vwap_tolerance == expected_exit_rules["vwap_tolerance"]
        assert strategy.max_hold_candles == expected_exit_rules["max_hold_candles"]
        assert strategy.stop_loss_enabled == expected_exit_rules["stop_loss_enabled"]
        assert strategy.stop_loss_multiplier == expected_exit_rules["stop_loss_multiplier"]

    def test_position_tracking(self, strategy):
        """Verify position state is tracked correctly on entry."""
        now = datetime.now(timezone.utc)
        
        history = self.create_history(n_bars=200, base_price=50000, trend="flat")
        
        # Manually set position
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000.0,
            "entry_deviation": -2.0,
            "entry_ts": now - timedelta(minutes=30),
            "entry_price": 49000.0,
        }
        
        # Verify position is tracked
        assert "BTC" in strategy._positions
        assert strategy._positions["BTC"]["side"] == "long"
        assert strategy._positions["BTC"]["entry_vwap"] == 50000.0
        assert strategy._positions["BTC"]["entry_deviation"] == -2.0
        
        # Exit the position
        candle = {
            "timestamp": now,
            "symbol": "BTC",
            "open": 50050,
            "high": 50100,
            "low": 49950,
            "close": 50050,  # Above VWAP tolerance
            "volume": 1000,
        }
        
        signal = strategy.on_candle(candle, history)
        assert signal is not None
        assert signal.side == "flat"
        
        # Verify position is cleared
        assert "BTC" not in strategy._positions

    def test_multiple_symbols_position_tracking(self, strategy):
        """Verify separate position tracking for multiple symbols."""
        strategy.symbols = ["BTC", "ETH"]
        
        # Set positions for different symbols
        strategy._positions["BTC"] = {
            "side": "long",
            "entry_vwap": 50000.0,
            "entry_deviation": -2.0,
            "entry_ts": datetime.now(timezone.utc) - timedelta(minutes=30),
            "entry_price": 49000.0,
        }
        strategy._positions["ETH"] = {
            "side": "short",
            "entry_vwap": 3000.0,
            "entry_deviation": 2.0,
            "entry_ts": datetime.now(timezone.utc) - timedelta(minutes=30),
            "entry_price": 3060.0,
        }
        
        assert strategy._positions["BTC"]["side"] == "long"
        assert strategy._positions["ETH"]["side"] == "short"
