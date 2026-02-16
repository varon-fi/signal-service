"""Volatility Expansion Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
import talib
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class VolatilityExpansionStrategy(BaseStrategy):
    """
    Volatility Expansion Strategy - Live Version
    
    Enters on volatility breakout after consolidation (squeeze).
    Uses Keltner Channels and Bollinger Bands to detect low volatility
    periods and enters on breakouts.
    
    Session: 14:00-18:00 UTC
    """
    
    name = "volatility_expansion"
    
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
        keltner_len = int(self.params.get("keltner_len", 20))
        atr_mult = float(self.params.get("atr_mult", 2.0))
        bb_len = int(self.params.get("bb_len", 20))
        bb_mult = float(self.params.get("bb_mult", 2.0))
        min_squeeze_bars = int(self.params.get("min_squeeze_bars", 3))
        
        # Calculate indicators
        closes = history['close'].values
        highs = history['high'].values
        lows = history['low'].values
        
        # Keltner Channels
        keltner_basis = talib.EMA(closes, timeperiod=keltner_len)
        atr = talib.ATR(highs, lows, closes, timeperiod=keltner_len)
        keltner_upper = keltner_basis + atr_mult * atr
        keltner_lower = keltner_basis - atr_mult * atr
        
        # Bollinger Bands
        bb_basis = talib.SMA(closes, timeperiod=bb_len)
        bb_stdev = talib.STDDEV(closes, timeperiod=bb_len)
        bb_upper = bb_basis + bb_mult * bb_stdev
        bb_lower = bb_basis - bb_mult * bb_stdev
        
        if pd.isna(keltner_upper[-1]) or pd.isna(bb_upper[-1]):
            return None
            
        # Squeeze detection (BB inside KC = low volatility)
        in_squeeze = (bb_upper < keltner_upper) & (bb_lower > keltner_lower)
        
        # Count consecutive squeeze bars
        squeeze_count = 0
        for i in range(1, min(len(in_squeeze), 20)):
            if in_squeeze[-i]:
                squeeze_count += 1
            else:
                break
                
        # Breakout detection
        # Breakout when NOT in squeeze now but WAS in squeeze
        curr_close = candle['close']
        prev_in_squeeze = in_squeeze[-2] if len(in_squeeze) > 1 else False
        curr_in_squeeze = in_squeeze[-1]
        
        breakout_up = (not curr_in_squeeze) and prev_in_squeeze and (curr_close > keltner_upper[-2])
        breakout_down = (not curr_in_squeeze) and prev_in_squeeze and (curr_close < keltner_lower[-2])
        
        # Entry conditions
        long_cond = breakout_up and (squeeze_count >= min_squeeze_bars)
        short_cond = breakout_down and (squeeze_count >= min_squeeze_bars)
        
        if long_cond:
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.75,
                meta={
                    "squeeze_bars": squeeze_count,
                    "keltner_upper": float(keltner_upper[-1]),
                    "keltner_lower": float(keltner_lower[-1]),
                    "in_session": True,
                    "breakout_type": "volatility_expansion"
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.75,
                meta={
                    "squeeze_bars": squeeze_count,
                    "keltner_upper": float(keltner_upper[-1]),
                    "keltner_lower": float(keltner_lower[-1]),
                    "in_session": True,
                    "breakout_type": "volatility_expansion"
                }
            )
            
        return None
