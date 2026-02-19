"""Tests for Low Volatility Momentum Strategy."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from signal_service.strategy.low_vol_momentum import LowVolMomentumStrategy


class TestLowVolMomentumStrategy:
    """Test Low Volatility Momentum Strategy."""
    
    @pytest.fixture
    def strategy(self):
        """Create strategy instance."""
        return LowVolMomentumStrategy(
            strategy_id="test-123",
            name="low_vol_momentum",
            version="1.0.0",
            symbols=["BTC"],
            timeframes=["15m"],
            params={
                "atr_period": 14,
                "lookback_days": 30,
                "low_vol_threshold": 40,
                "momentum_lookback": 48,
                "stop_loss_pct": 2.0
            }
        )
    
    @pytest.fixture
    def sample_history(self):
        """Create sample OHLC history."""
        np.random.seed(42)
        n = 500  # 500 periods of 15m = ~5 days
        
        # Generate price with low volatility trend
        base_price = 50000
        returns = np.random.normal(0.0001, 0.005, n)  # Low volatility
        prices = base_price * np.exp(np.cumsum(returns))
        
        timestamps = [datetime.now() - timedelta(minutes=15*(n-i)) for i in range(n)]
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': prices * (1 + np.random.normal(0, 0.001, n)),
            'high': prices * (1 + abs(np.random.normal(0, 0.003, n))),
            'low': prices * (1 - abs(np.random.normal(0, 0.003, n))),
            'close': prices,
            'volume': np.random.uniform(100, 1000, n)
        })
        
        return df
    
    def test_strategy_creation(self, strategy):
        """Test strategy is created with correct parameters."""
        assert strategy.name == "low_vol_momentum"
        assert strategy.atr_period == 14
        assert strategy.low_vol_threshold == 40
        assert strategy.momentum_lookback == 48
        
    def test_insufficient_data(self, strategy):
        """Test returns None with insufficient data."""
        candle = {
            'timestamp': datetime.now(),
            'open': 50000,
            'high': 51000,
            'low': 49000,
            'close': 50500,
            'volume': 1000
        }
        history = pd.DataFrame({
            'timestamp': [datetime.now()],
            'open': [50000],
            'high': [51000],
            'low': [49000],
            'close': [50500],
            'volume': [1000]
        })
        
        signal = strategy.on_candle(candle, history)
        assert signal is None
        
    def test_signal_in_low_vol_regime(self, strategy, sample_history):
        """Test signal generation in low volatility regime."""
        # Create a bullish candle with positive momentum
        last_price = sample_history['close'].iloc[-1]
        
        candle = {
            'timestamp': datetime.now(),
            'open': last_price * 0.995,
            'high': last_price * 1.01,
            'low': last_price * 0.99,
            'close': last_price * 1.005,  # Bullish close
            'volume': 1000
        }
        
        signal = strategy.on_candle(candle, sample_history)
        
        # Should generate signal in low vol regime with positive momentum
        if signal:
            assert signal.side in ['long', 'short']
            assert signal.price == candle['close']
            assert signal.confidence > 0
            assert 'atr_percentile' in signal.meta
            assert signal.meta['regime'] == 'low'
            
    def test_no_signal_in_high_vol(self, strategy):
        """Test no signal in high volatility regime."""
        np.random.seed(42)
        n = 500
        
        # Generate high volatility prices
        base_price = 50000
        returns = np.random.normal(0, 0.05, n)  # High volatility
        prices = base_price * np.exp(np.cumsum(returns))
        
        timestamps = [datetime.now() - timedelta(minutes=15*(n-i)) for i in range(n)]
        
        history = pd.DataFrame({
            'timestamp': timestamps,
            'open': prices * (1 + np.random.normal(0, 0.01, n)),
            'high': prices * (1 + abs(np.random.normal(0, 0.03, n))),
            'low': prices * (1 - abs(np.random.normal(0, 0.03, n))),
            'close': prices,
            'volume': np.random.uniform(100, 1000, n)
        })
        
        candle = {
            'timestamp': datetime.now(),
            'open': prices[-1] * 0.99,
            'high': prices[-1] * 1.02,
            'low': prices[-1] * 0.98,
            'close': prices[-1] * 1.01,
            'volume': 1000
        }
        
        signal = strategy.on_candle(candle, history)
        
        # Should not generate signal in high vol regime
        assert signal is None
