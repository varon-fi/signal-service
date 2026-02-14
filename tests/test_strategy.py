"""Tests for Signal Service."""

import pytest
import pandas as pd
import numpy as np

from varon_fi import Signal
from signal_service.strategy.mtf_confluence import MtfConfluenceStrategy


class TestMtfConfluenceStrategy:
    """Test MTF Confluence strategy."""
    
    @pytest.fixture
    def strategy(self):
        return MtfConfluenceStrategy(
            strategy_id="test-123",
            name="mtf_confluence",
            version="1.0.0",
            symbols=["BTC"],
            timeframes=["5m"],
            params={
                "htf_ema_len": 50,
                "htf_rsi_len": 14,
                "htf_rsi_mid": 50,
                "ltf_ema_len": 20,
                "pullback_pct": 0.3,
            }
        )
        
    @pytest.fixture
    def sample_history(self):
        """Generate sample OHLC history."""
        np.random.seed(42)
        n = 300
        base_price = 50000
        
        # Generate trending data
        trend = np.linspace(0, 1000, n)
        noise = np.random.randn(n) * 100
        closes = base_price + trend + noise
        
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=n, freq='5min'),
            'open': closes - np.abs(np.random.randn(n) * 50),
            'high': closes + np.abs(np.random.randn(n) * 100),
            'low': closes - np.abs(np.random.randn(n) * 100),
            'close': closes,
            'volume': np.random.randint(1000, 10000, n),
        })
        return df
        
    def test_strategy_creation(self, strategy):
        """Test strategy initializes correctly."""
        assert strategy.strategy_id == "test-123"
        assert strategy.name == "mtf_confluence"
        assert strategy.version == "1.0.0"
        assert "BTC" in strategy.symbols
        
    def test_on_candle_insufficient_data(self, strategy):
        """Test returns None with insufficient history."""
        small_history = pd.DataFrame({
            'timestamp': [1, 2],
            'open': [100, 101],
            'high': [105, 106],
            'low': [99, 100],
            'close': [102, 103],
            'volume': [1000, 2000],
        })
        candle = {'open': 104, 'high': 107, 'low': 103, 'close': 106, 'volume': 3000}
        signal = strategy.on_candle(candle, small_history)
        assert signal is None
        
    def test_signal_properties(self, strategy, sample_history):
        """Test signal has required properties."""
        candle = sample_history.iloc[-1].to_dict()
        history = sample_history.iloc[:-1]
        
        signal = strategy.on_candle(candle, history)
        
        if signal:  # Signal may or may not be generated depending on conditions
            assert signal.side in ['long', 'short']
            assert signal.price is not None
            assert 0 <= signal.confidence <= 1
            assert 'htf_ema' in signal.meta
            assert 'htf_rsi' in signal.meta
            
    def test_resample_htf(self, strategy, sample_history):
        """Test HTF resampling."""
        closes = sample_history['close']
        htf = strategy._resample_htf(closes)
        
        # Should reduce length by factor of 3
        expected_len = len(closes) // 3
        assert len(htf) == expected_len
        
        # Last value should be last of every 3
        assert htf.iloc[-1] == closes.iloc[len(closes) - 1 - (len(closes) % 3)]


class TestSignalDataclass:
    """Test Signal dataclass."""
    
    def test_signal_creation(self):
        """Test signal can be created."""
        signal = Signal(side="long", price=50000, confidence=0.8)
        assert signal.side == "long"
        assert signal.price == 50000
        assert signal.confidence == 0.8
        assert signal.idempotency_key is not None
        assert signal.correlation_id is not None
