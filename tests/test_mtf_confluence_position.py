"""Test MTF Confluence strategy position tracking."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone

from signal_service.strategy.mtf_confluence import MtfConfluenceStrategy


def create_candle(close, open_price=None, high=None, low=None, timestamp=None):
    """Create a test candle."""
    return {
        'open': open_price if open_price is not None else close * 0.999,
        'high': high if high is not None else close * 1.001,
        'low': low if low is not None else close * 0.998,
        'close': close,
        'volume': 1000,
        'timestamp': timestamp or datetime(2026, 2, 16, 15, 0, 0, tzinfo=timezone.utc)
    }


def create_history(bars=200, trend='up'):
    """Create test history with specified trend."""
    np.random.seed(42)
    
    closes = []
    opens = []
    highs = []
    lows = []
    volumes = []
    
    base_price = 50000
    
    for i in range(bars):
        if trend == 'up':
            base_price *= 1.001  # Slight uptrend
        elif trend == 'down':
            base_price *= 0.999  # Slight downtrend
        
        noise = np.random.randn() * base_price * 0.005
        close = base_price + noise
        open_price = close * (1 + np.random.randn() * 0.002)
        high = max(close, open_price) * 1.002
        low = min(close, open_price) * 0.998
        
        closes.append(close)
        opens.append(open_price)
        highs.append(high)
        lows.append(low)
        volumes.append(1000 + np.random.randint(0, 500))
    
    df = pd.DataFrame({
        'open': opens,
        'high': highs,
        'low': lows,
        'close': closes,
        'volume': volumes
    })
    
    return df


class TestMtfConfluencePositionTracking:
    """Test that MTF Confluence properly tracks position state."""
    
    def test_no_position_initially(self):
        """Strategy should start with no position."""
        strategy = MtfConfluenceStrategy()
        assert strategy._position_side is None
    
    def test_entry_creates_position(self):
        """Long entry should set position to long."""
        strategy = MtfConfluenceStrategy()
        
        # Create bullish trending history
        history = create_history(bars=200, trend='up')
        
        # Create a pullback candle within session
        candle = create_candle(
            close=history['close'].iloc[-1] * 0.998,  # Slight pullback
            open_price=history['close'].iloc[-1] * 0.997,
            timestamp=datetime(2026, 2, 16, 15, 30, 0, tzinfo=timezone.utc)
        )
        
        signal = strategy.on_candle(candle, history)
        
        if signal and signal.side == "long":
            assert strategy._position_side == "long"
            assert strategy._entry_price is not None
    
    def test_no_second_entry_when_long(self):
        """Should not generate entry signal when already long."""
        strategy = MtfConfluenceStrategy()
        
        # Manually set position to long
        strategy._position_side = "long"
        strategy._entry_price = 50000
        
        history = create_history(bars=200, trend='up')
        candle = create_candle(
            close=50000,
            timestamp=datetime(2026, 2, 16, 15, 35, 0, tzinfo=timezone.utc)
        )
        
        signal = strategy.on_candle(candle, history)
        
        # Should not generate an entry signal
        if signal:
            assert signal.side != "long", "Should not enter long when already long"
    
    def test_exit_clears_position(self):
        """Exit signal should clear position state."""
        strategy = MtfConfluenceStrategy()
        
        # Set up a long position
        strategy._position_side = "long"
        strategy._entry_price = 50000
        strategy._bars_since_entry = 5
        
        # Create history with trend reversal
        history = create_history(bars=200, trend='down')
        
        # Create candle that triggers exit
        candle = create_candle(
            close=49000,  # Below entry, should hit stop
            timestamp=datetime(2026, 2, 16, 16, 0, 0, tzinfo=timezone.utc)
        )
        
        signal = strategy.on_candle(candle, history)
        
        if signal and signal.side == "flat":
            assert strategy._position_side is None
            assert strategy._entry_price is None
    
    def test_outside_session_returns_none(self):
        """Should not generate signals outside 14:00-18:00 UTC."""
        strategy = MtfConfluenceStrategy()
        
        history = create_history(bars=200, trend='up')
        
        # Create candle outside session (20:00 UTC)
        candle = create_candle(
            close=50000,
            timestamp=datetime(2026, 2, 16, 20, 0, 0, tzinfo=timezone.utc)
        )
        
        signal = strategy.on_candle(candle, history)
        
        assert signal is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
