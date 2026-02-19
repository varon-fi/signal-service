"""Low Volatility Momentum Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
import talib
import numpy as np
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class LowVolMomentumStrategy(BaseStrategy):
    """
    Low Volatility Momentum Strategy - Live Version
    
    Trade momentum ONLY in low volatility regimes (bottom 40% ATR percentile).
    Skip trades during high volatility.
    
    Backtest Results (90 days, 6 symbols):
    - 2,348 trades
    - 64.5% win rate
    - +1,312% PnL
    - 1.23 profit factor
    
    Best Params:
    - low_vol_threshold: 40 (trade when vol < 40th percentile)
    - momentum_lookback: 48 hours
    - atr_period: 14
    - lookback_days: 30 (for percentile calc)
    """
    
    name = "low_vol_momentum"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.utc = pytz.UTC
        
        # Strategy parameters
        self.atr_period = int(self.params.get("atr_period", 14))
        self.lookback_days = int(self.params.get("lookback_days", 30))
        self.low_vol_threshold = float(self.params.get("low_vol_threshold", 40))
        self.momentum_lookback = int(self.params.get("momentum_lookback", 48))
        self.stop_loss_pct = float(self.params.get("stop_loss_pct", 2.0))
        
        # Session times (optional)
        session_start = self.params.get("session_start")
        session_end = self.params.get("session_end")
        self.session_start = self._parse_time(session_start) if session_start else None
        self.session_end = self._parse_time(session_end) if session_end else None
        
    def _parse_time(self, time_str: str) -> time:
        """Parse time string to time object."""
        if isinstance(time_str, str):
            return datetime.strptime(time_str, "%H:%M").time()
        return time_str
        
    def _in_session(self, ts) -> bool:
        """Check if timestamp is within trading session."""
        if self.session_start is None or self.session_end is None:
            return True
            
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
        
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(period).mean()
        
    def _get_vol_regime(self, history: pd.DataFrame) -> tuple:
        """
        Determine volatility regime.
        
        Returns:
            (regime: str, atr_percentile: float)
            regime: 'low', 'mid', 'high'
        """
        if len(history) < self.atr_period + 10:
            return 'unknown', 50.0
            
        # Calculate ATR
        atr = self._calculate_atr(history, self.atr_period)
        atr_pct = (atr / history['close']) * 100
        
        # Need enough history for percentile calculation
        lookback_periods = self.lookback_days * 24 * 4  # 15m candles
        if len(atr_pct) < lookback_periods:
            lookback_periods = len(atr_pct)
            
        if lookback_periods < self.atr_period * 2:
            return 'unknown', 50.0
            
        # Calculate percentile of current ATR
        current_atr_pct = atr_pct.iloc[-1]
        atr_history = atr_pct.iloc[-lookback_periods:].dropna()
        
        if len(atr_history) < 10:
            return 'unknown', 50.0
            
        percentile = (atr_history < current_atr_pct).mean() * 100
        
        # Classify regime
        if percentile < self.low_vol_threshold:
            return 'low', percentile
        elif percentile > 70:  # High vol threshold
            return 'high', percentile
        else:
            return 'mid', percentile
        
    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        if len(history) < self.atr_period * 2:
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
                
        # Get volatility regime
        regime, atr_percentile = self._get_vol_regime(history)
        
        # Only trade in low volatility regime
        if regime != 'low':
            return None
            
        # Calculate momentum (48h lookback = 192 periods of 15m)
        momentum_periods = self.momentum_lookback * 4  # 15m candles
        if len(history) < momentum_periods:
            return None
            
        momentum = (history['close'].iloc[-1] - history['close'].iloc[-momentum_periods]) / history['close'].iloc[-momentum_periods]
        
        curr_close = candle['close']
        curr_open = candle['open']
        
        # Entry conditions
        # Long: Positive momentum, bullish candle
        long_cond = (
            momentum > 0.01 and  # 1% momentum over lookback
            curr_close > curr_open  # Bullish candle
        )
        
        # Short: Negative momentum, bearish candle
        short_cond = (
            momentum < -0.01 and
            curr_close < curr_open  # Bearish candle
        )
        
        if long_cond:
            return Signal(
                side="long",
                price=curr_close,
                confidence=min(0.5 + abs(momentum) * 10, 0.9),  # Higher momentum = higher confidence
                meta={
                    "momentum": float(momentum),
                    "atr_percentile": float(atr_percentile),
                    "regime": regime,
                    "momentum_lookback_hours": self.momentum_lookback,
                    "strategy_type": "low_vol_momentum",
                    "in_session": True
                }
            )
            
        if short_cond:
            return Signal(
                side="short",
                price=curr_close,
                confidence=min(0.5 + abs(momentum) * 10, 0.9),
                meta={
                    "momentum": float(momentum),
                    "atr_percentile": float(atr_percentile),
                    "regime": regime,
                    "momentum_lookback_hours": self.momentum_lookback,
                    "strategy_type": "low_vol_momentum",
                    "in_session": True
                }
            )
            
        return None
