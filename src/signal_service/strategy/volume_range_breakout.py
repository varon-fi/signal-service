"""Volume-Range Breakout Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register
from varon_fi.ta import atr

logger = get_logger(__name__)


@register
class VolumeRangeBreakoutStrategy(BaseStrategy):
    """
    Volume-Range Breakout Strategy - Live Version
    
    Breakout strategy that enters when price breaks out of a defined
    range with above-average volume confirmation.
    
    Session: 14:00-18:00 UTC
    """
    
    name = "volume_range_breakout"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.utc = pytz.UTC
        self.session_start = time(14, 0)
        self.session_end = time(18, 0)
        
    def _in_session(self, ts) -> bool:
        """Check if timestamp is within trading session."""
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts, self.utc)
        elif hasattr(ts, 'seconds') and hasattr(ts, 'nanos'):
            ts = datetime.fromtimestamp(ts.seconds, self.utc)
        elif isinstance(ts, str):
            ts = pd.to_datetime(ts)
        elif hasattr(ts, 'ToDatetime'):
            ts = ts.ToDatetime(tzinfo=self.utc)
            
        if not isinstance(ts, datetime):
            return True
            
        try:
            if ts.tzinfo is None:
                ts = self.utc.localize(ts)
            else:
                ts = ts.astimezone(self.utc)
        except (AttributeError, ValueError):
            pass
            
        current_time = ts.time()
        return self.session_start <= current_time <= self.session_end
        
    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        if len(history) < 50:
            return None
            
        # Convert Decimal columns to float
        history = history.copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in history.columns:
                history[col] = history[col].astype(float)
                
        numeric_fields = {'open', 'high', 'low', 'close', 'volume', 'price'}
        candle = {k: float(v) if k in numeric_fields and v is not None else v 
                  for k, v in candle.items()}
                  
        # Session filter
        candle_ts = candle.get('timestamp') or candle.get('ts')
        if candle_ts is not None:
            if not self._in_session(candle_ts):
                return None
                
        # Parameters
        lookback = int(self.params.get("lookback", 20))
        volume_threshold = float(self.params.get("volume_threshold", 1.5))
        min_range_pct = float(self.params.get("min_range_pct", 0.3))
        volatility_filter = float(self.params.get("volatility_filter", 2.5))
        
        # Calculate indicators
        closes = history['close'].values
        highs = history['high'].values
        lows = history['low'].values
        volumes = history['volume'].values
        
        # Range detection
        range_high = pd.Series(highs).rolling(lookback).max().values
        range_low = pd.Series(lows).rolling(lookback).min().values
        
        # Volume analysis
        avg_volume = pd.Series(volumes).rolling(lookback).mean().values
        
        # Volatility filter (ATR-based)
        atr_vals = atr(highs, lows, closes, 14)
        volatility = (atr_vals / closes) * 100
        
        if pd.isna(range_high[-1]) or pd.isna(avg_volume[-1]) or pd.isna(volatility[-1]):
            return None
            
        # Current values
        curr_close = candle['close']
        curr_volume = candle['volume']
        prev_range_high = range_high[-2] if len(range_high) > 1 else range_high[-1]
        prev_range_low = range_low[-2] if len(range_low) > 1 else range_low[-1]
        curr_volatility = volatility[-1]
        
        # Range size check
        range_size = ((prev_range_high - prev_range_low) / prev_range_low) * 100
        
        # Volume check
        volume_ok = curr_volume > (avg_volume[-1] * volume_threshold)
        
        # Volatility check
        volatility_ok = curr_volatility < volatility_filter
        
        # Breakout conditions
        # Crossover: close > previous range high AND close[-1] <= range high
        prev_close = closes[-2] if len(closes) > 1 else closes[-1]
        breakout_long = (curr_close > prev_range_high) and (prev_close <= prev_range_high)
        breakout_short = (curr_close < prev_range_low) and (prev_close >= prev_range_low)
        
        # Entry conditions
        long_cond = breakout_long and volume_ok and (range_size > min_range_pct) and volatility_ok
        short_cond = breakout_short and volume_ok and (range_size > min_range_pct) and volatility_ok
        
        if long_cond:
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.7,
                meta={
                    "range_high": float(prev_range_high),
                    "range_low": float(prev_range_low),
                    "range_size_pct": float(range_size),
                    "volume_ratio": float(curr_volume / avg_volume[-1]) if avg_volume[-1] > 0 else 0,
                    "volatility_pct": float(curr_volatility),
                    "in_session": True,
                    "breakout_type": "range_breakout"
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.7,
                meta={
                    "range_high": float(prev_range_high),
                    "range_low": float(prev_range_low),
                    "range_size_pct": float(range_size),
                    "volume_ratio": float(curr_volume / avg_volume[-1]) if avg_volume[-1] > 0 else 0,
                    "volatility_pct": float(curr_volatility),
                    "in_session": True,
                    "breakout_type": "range_breakout"
                }
            )
            
        return None
