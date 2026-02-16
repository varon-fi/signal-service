"""MTF Confluence Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
import talib
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class MtfConfluenceStrategy(BaseStrategy):
    name = "mtf_confluence"
    """
    Multi-Timeframe Confluence Strategy - Live Version
    
    Uses 15m HTF for trend direction (EMA + RSI) and 5m LTF for entry precision.
    Enters on pullbacks in the direction of the higher timeframe trend.
    
    Session: 14:00-18:00 UTC (matches Pine Script)
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.htf_mult = 3  # 15m = 3x 5m candles
        self.utc = pytz.UTC
        # Session filter: 14:00-18:00 UTC (matches Pine Script)
        self.session_start = time(14, 0)  # 14:00 UTC
        self.session_end = time(18, 0)    # 18:00 UTC
        # Position tracking - only generate entry signals when flat
        self._position_side = None  # None = flat, "long", or "short"
        self._entry_price = None
        self._bars_since_entry = 0
        
    def _in_session(self, ts) -> bool:
        """Check if timestamp is within trading session (14:00-18:00 UTC)."""
        # Handle different timestamp types
        if isinstance(ts, (int, float)):
            # Unix timestamp (seconds)
            ts = datetime.fromtimestamp(ts, self.utc)
        elif hasattr(ts, 'seconds') and hasattr(ts, 'nanos'):  # Protobuf Timestamp
            # Convert seconds to datetime (ignore nanos for session check)
            ts = datetime.fromtimestamp(ts.seconds, self.utc)
        elif isinstance(ts, str):
            ts = pd.to_datetime(ts)
        elif hasattr(ts, 'ToDatetime'):  # Protobuf Timestamp with ToDatetime method
            ts = ts.ToDatetime(tzinfo=self.utc)
        
        # Ensure datetime
        if not isinstance(ts, datetime):
            return True  # Allow if we can't parse
        
        # Ensure timestamp is UTC
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
        """Process new candle and return signal if conditions met.
        
        Only generates entry signals when flat (no position).
        Tracks position state internally to prevent signal spam.
        """
        if len(history) < 200:
            return None
            
        # Convert Decimal columns to float for TA-Lib compatibility
        history = history.copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in history.columns:
                history[col] = history[col].astype(float)
        
        # Convert numeric candle values to float
        numeric_fields = {'open', 'high', 'low', 'close', 'volume', 'price'}
        candle = {k: float(v) if k in numeric_fields and v is not None else v 
                  for k, v in candle.items()}
        
        # Session filter (14:00-18:00 UTC) - matches Pine Script
        candle_ts = candle.get('timestamp') or candle.get('ts')
        if candle_ts is not None:
            if not self._in_session(candle_ts):
                return None  # Outside trading session
            
        # Parameters
        htf_ema_len = int(self.params.get("htf_ema_len", 50))
        htf_rsi_len = int(self.params.get("htf_rsi_len", 14))
        htf_rsi_mid = int(self.params.get("htf_rsi_mid", 50))
        ltf_ema_len = int(self.params.get("ltf_ema_len", 20))
        pullback_pct = float(self.params.get("pullback_pct", 0.3))
        
        # Build HTF OHLC (15m from 5m data)
        htf_closes = self._resample_htf(history['close'])
        htf_highs = self._resample_htf(history['high'], agg='max')
        htf_lows = self._resample_htf(history['low'], agg='min')
        
        if len(htf_closes) < htf_ema_len + 2:
            return None
            
        # HTF indicators
        htf_ema = talib.EMA(htf_closes, timeperiod=htf_ema_len)
        htf_rsi = talib.RSI(htf_closes, timeperiod=htf_rsi_len)
        
        # LTF indicators
        ltf_ema = talib.EMA(history['close'], timeperiod=ltf_ema_len)
        
        # Get current values
        curr_close = candle['close']
        curr_open = candle['open']
        curr_ltf_ema = ltf_ema.iloc[-1]
        
        # Get last COMPLETED HTF values (index -2 to avoid partial bar)
        htf_close = htf_closes.iloc[-2]
        htf_ema_val = htf_ema.iloc[-2]
        htf_rsi_val = htf_rsi.iloc[-2]
        
        if pd.isna(htf_ema_val) or pd.isna(htf_rsi_val) or pd.isna(curr_ltf_ema):
            return None
            
        # Trend determination
        htf_bullish = (htf_close > htf_ema_val) and (htf_rsi_val > htf_rsi_mid)
        htf_bearish = (htf_close < htf_ema_val) and (htf_rsi_val < htf_rsi_mid)
        
        # Increment bars since entry
        if self._position_side is not None:
            self._bars_since_entry += 1
        
        # Check for exit conditions if in a position
        if self._position_side == "long":
            # Exit on trend reversal or stop loss
            stop_loss_hit = self._entry_price and curr_close < self._entry_price * 0.995  # 0.5% stop
            trend_reversed = htf_bearish  # HTF turned bearish
            
            if stop_loss_hit or trend_reversed:
                self._position_side = None
                self._entry_price = None
                self._bars_since_entry = 0
                return Signal(
                    side="flat",
                    price=curr_close,
                    confidence=0.7,
                    meta={
                        "exit_reason": "stop_loss" if stop_loss_hit else "trend_reversal",
                        "htf_ema": float(htf_ema_val),
                        "htf_rsi": float(htf_rsi_val),
                        "entry_price": self._entry_price,
                    }
                )
            return None  # Stay in position
            
        if self._position_side == "short":
            # Exit on trend reversal or stop loss
            stop_loss_hit = self._entry_price and curr_close > self._entry_price * 1.005  # 0.5% stop
            trend_reversed = htf_bullish  # HTF turned bullish
            
            if stop_loss_hit or trend_reversed:
                self._position_side = None
                self._entry_price = None
                self._bars_since_entry = 0
                return Signal(
                    side="flat",
                    price=curr_close,
                    confidence=0.7,
                    meta={
                        "exit_reason": "stop_loss" if stop_loss_hit else "trend_reversal",
                        "htf_ema": float(htf_ema_val),
                        "htf_rsi": float(htf_rsi_val),
                        "entry_price": self._entry_price,
                    }
                )
            return None  # Stay in position
        
        # Only check entry conditions if flat (no position)
        if self._position_side is not None:
            return None
            
        if not (htf_bullish or htf_bearish):
            return None
            
        # Pullback calculation
        recent_high_ltf = history['high'].iloc[-5:].max()
        recent_low_ltf = history['low'].iloc[-5:].min()
        
        EMA_PROXIMITY_BUFFER = 0.001  # 0.1% buffer
        pullback_threshold = pullback_pct / 100.0
        
        # Long pullback conditions
        price_near_ema_long = curr_close < curr_ltf_ema * (1 + EMA_PROXIMITY_BUFFER)
        breakout_above_high = curr_close > recent_high_ltf * (1 - pullback_threshold)
        pullback_long = (price_near_ema_long or breakout_above_high) and (curr_close > curr_open)
        
        # Short pullback conditions  
        price_near_ema_short = curr_close > curr_ltf_ema * (1 - EMA_PROXIMITY_BUFFER)
        breakdown_below_low = curr_close < recent_low_ltf * (1 + pullback_threshold)
        pullback_short = (price_near_ema_short or breakdown_below_low) and (curr_close < curr_open)
        
        # Entry conditions
        long_cond = htf_bullish and pullback_long and (curr_close > curr_ltf_ema)
        short_cond = htf_bearish and pullback_short and (curr_close < curr_ltf_ema)
        
        if long_cond:
            self._position_side = "long"
            self._entry_price = curr_close
            self._bars_since_entry = 0
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.7,
                meta={
                    "htf_ema": float(htf_ema_val),
                    "htf_rsi": float(htf_rsi_val),
                    "ltf_ema": float(curr_ltf_ema),
                    "pullback": True,
                    "in_session": True,
                    "session": "14:00-18:00 UTC",
                    "entry": True,
                }
            )
            
        if short_cond:
            self._position_side = "short"
            self._entry_price = curr_close
            self._bars_since_entry = 0
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.7,
                meta={
                    "htf_ema": float(htf_ema_val),
                    "htf_rsi": float(htf_rsi_val),
                    "ltf_ema": float(curr_ltf_ema),
                    "pullback": True,
                    "in_session": True,
                    "session": "14:00-18:00 UTC",
                    "entry": True,
                }
            )
            
        return None
        
    def _resample_htf(self, series: pd.Series, agg: str = 'last') -> pd.Series:
        """Resample 5m series to 15m HTF."""
        groups = series.index // self.htf_mult
        if agg == 'last':
            return series.groupby(groups).last().reset_index(drop=True)
        elif agg == 'max':
            return series.groupby(groups).max().reset_index(drop=True)
        elif agg == 'min':
            return series.groupby(groups).min().reset_index(drop=True)
        elif agg == 'first':
            return series.groupby(groups).first().reset_index(drop=True)
        return series.groupby(groups).last().reset_index(drop=True)
