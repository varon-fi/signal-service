"""Momentum Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register
from varon_fi.ta import rsi

logger = get_logger(__name__)


@register
class MomentumStrategy(BaseStrategy):
    """
    Momentum Strategy - Live Version
    
    RSI-based momentum strategy using VWAP bands for mean-reversion
    entries in the direction of momentum.
    
    Session: 14:00-18:00 UTC
    """
    
    name = "momentum"
    
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
        
    def _calculate_vwap(self, df: pd.DataFrame) -> pd.Series:
        """Calculate VWAP (Volume Weighted Average Price)."""
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return vwap
        
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
        rsi_length = int(self.params.get("rsi_length", 14))
        rsi_overbought = int(self.params.get("rsi_overbought", 65))
        rsi_oversold = int(self.params.get("rsi_oversold", 35))
        vwap_deviation = float(self.params.get("vwap_deviation", 0.5))
        
        # Calculate indicators
        closes = history['close'].values
        
        # RSI
        rsi_vals = rsi(closes, rsi_length)
        
        # VWAP calculation
        vwap_series = self._calculate_vwap(history)
        vwap = vwap_series.iloc[-1]
        
        # VWAP bands
        vwap_upper = vwap * (1 + vwap_deviation / 100)
        vwap_lower = vwap * (1 - vwap_deviation / 100)
        
        if pd.isna(rsi_vals[-1]) or pd.isna(vwap):
            return None
            
        # Current values
        curr_close = candle['close']
        curr_open = candle['open']
        curr_rsi = rsi_vals[-1]
        
        # Entry conditions
        # Long: Price above VWAP lower, RSI between oversold and 50, bullish candle
        long_cond = (
            curr_close > vwap_lower and 
            curr_rsi > rsi_oversold and 
            curr_rsi < 50 and 
            curr_close > curr_open
        )
        
        # Short: Price below VWAP upper, RSI between overbought and 50, bearish candle
        short_cond = (
            curr_close < vwap_upper and 
            curr_rsi < rsi_overbought and 
            curr_rsi > 50 and 
            curr_close < curr_open
        )
        
        if long_cond:
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.65,
                meta={
                    "rsi": float(curr_rsi),
                    "vwap": float(vwap),
                    "vwap_upper": float(vwap_upper),
                    "vwap_lower": float(vwap_lower),
                    "in_session": True,
                    "momentum_type": "rsi_vwap"
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.65,
                meta={
                    "rsi": float(curr_rsi),
                    "vwap": float(vwap),
                    "vwap_upper": float(vwap_upper),
                    "vwap_lower": float(vwap_lower),
                    "in_session": True,
                    "momentum_type": "rsi_vwap"
                }
            )
            
        return None
