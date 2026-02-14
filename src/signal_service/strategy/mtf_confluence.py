"""MTF Confluence Strategy implementation for live trading."""

from typing import Optional

import pandas as pd
import talib

from varon_fi import BaseStrategy, Signal, register


@register
class MtfConfluenceStrategy(BaseStrategy):
    name = "mtf_confluence"
    """
    Multi-Timeframe Confluence Strategy - Live Version
    
    Uses 15m HTF for trend direction (EMA + RSI) and 5m LTF for entry precision.
    Enters on pullbacks in the direction of the higher timeframe trend.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.htf_mult = 3  # 15m = 3x 5m candles
        
    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        if len(history) < 200:
            return None
            
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
            return Signal(
                side="long",
                price=curr_close,
                confidence=0.7,
                meta={
                    "htf_ema": float(htf_ema_val),
                    "htf_rsi": float(htf_rsi_val),
                    "ltf_ema": float(curr_ltf_ema),
                    "pullback": True,
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=0.7,
                meta={
                    "htf_ema": float(htf_ema_val),
                    "htf_rsi": float(htf_rsi_val),
                    "ltf_ema": float(curr_ltf_ema),
                    "pullback": True,
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
