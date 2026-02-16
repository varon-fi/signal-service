"""ATR Breakout Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
import talib
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class AtrBreakoutStrategy(BaseStrategy):
    """
    ATR Breakout Strategy - Live Version
    
    ATR-based breakout strategy with EMA trend filter.
    Enters on breakouts from ATR-derived bands in the direction
    of the EMA trend.
    
    Session: 14:00-18:00 UTC
    """
    
    name = "atr_breakout"
    
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
        if len(history) < 100:
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
        atr_length = int(self.params.get("atr_length", 14))
        atr_mult = float(self.params.get("atr_mult", 1.5))
        ema_filter = int(self.params.get("ema_filter", 50))
        
        # Calculate indicators
        closes = history['close'].values
        highs = history['high'].values
        lows = history['low'].values
        
        # ATR
        atr = talib.ATR(highs, lows, closes, timeperiod=atr_length)
        
        # EMA filter
        ema = talib.EMA(closes, timeperiod=ema_filter)
        
        # Bands calculation
        highest_high = talib.MAX(highs, timeperiod=atr_length)
        lowest_low = talib.MIN(lows, timeperiod=atr_length)
        
        upper_band = highest_high + atr * 0.5
        lower_band = lowest_low - atr * 0.5
        
        if pd.isna(upper_band[-1]) or pd.isna(ema[-1]):
            return None
            
        # Current values
        curr_close = candle['close']
        prev_close = closes[-2] if len(closes) > 1 else closes[-1]
        prev_upper_band = upper_band[-2] if len(upper_band) > 1 else upper_band[-1]
        prev_lower_band = lower_band[-2] if len(lower_band) > 1 else lower_band[-1]
        curr_ema = ema[-1]
        
        # Trend filter
        above_ema = curr_close > curr_ema
        below_ema = curr_close < curr_ema
        
        # Breakout detection (crossover)
        breakout_long = (curr_close > prev_upper_band) and (prev_close <= prev_upper_band)
        breakout_short = (curr_close < prev_lower_band) and (prev_close >= prev_lower_band)
        
        # Entry conditions
        long_cond = breakout_long and above_ema
        short_cond = breakout_short and below_ema
        
        if long_cond:
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.7,
                meta={
                    "upper_band": float(prev_upper_band),
                    "lower_band": float(prev_lower_band),
                    "ema_filter": float(curr_ema),
                    "atr": float(atr[-1]),
                    "in_session": True,
                    "breakout_type": "atr_breakout",
                    "trend_aligned": True
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.7,
                meta={
                    "upper_band": float(prev_upper_band),
                    "lower_band": float(prev_lower_band),
                    "ema_filter": float(curr_ema),
                    "atr": float(atr[-1]),
                    "in_session": True,
                    "breakout_type": "atr_breakout",
                    "trend_aligned": True
                }
            )
            
        return None
