"""Range Mean Reversion Strategy with Exit Logic for live trading."""

from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class RangeMeanReversionStrategy(BaseStrategy):
    """
    Range Mean Reversion Scalper (VWAP Proxy) with Exit Logic

    Enters when price over-extends from short-term VWAP and
    RSI shows extreme conditions. Exits near VWAP, at max hold time,
    or via stop loss.

    Based on: docs/research/scalping-strategies.md #2
    Version: 1.1.0 (adds exits)
    """
    name = "range_mean_reversion"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Entry parameters
        self.vwap_lookback = int(self.params.get("vwap_lookback", 20))
        self.rsi_period = int(self.params.get("rsi_period", 14))
        self.rsi_oversold = float(self.params.get("rsi_oversold", 30))
        self.rsi_overbought = float(self.params.get("rsi_overbought", 70))
        self.deviation_pct = float(self.params.get("deviation_pct", 1.0))
        self.ema_filter_period = int(self.params.get("ema_filter_period", 50))
        self.max_atr_pct = float(self.params.get("max_atr_pct", 2.0))
        
        # Exit parameters (new in v1.1.0)
        self.vwap_tolerance = float(self.params.get("vwap_tolerance", 0.002))
        self.max_hold_candles = int(self.params.get("max_hold_candles", 15))
        self.stop_loss_enabled = bool(self.params.get("stop_loss_enabled", True))
        self.stop_loss_multiplier = float(self.params.get("stop_loss_multiplier", 1.5))
        
        # Track position state for exits (per symbol)
        self._positions: dict[str, dict] = {}

    def _calculate_ema(self, series: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average."""
        return series.ewm(span=period, adjust=False).mean()
    
    def _calculate_rsi(self, series: pd.Series, period: int) -> pd.Series:
        """Calculate Relative Strength Index."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(period).mean()

    def _position_key(self, candle: dict) -> str:
        """Generate position tracking key for symbol."""
        symbol = candle.get("symbol")
        if symbol:
            return symbol
        if self.symbols:
            return self.symbols[0]
        return "default"

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        # Convert to DataFrame if needed and ensure numeric types
        history = history.copy() if isinstance(history, pd.DataFrame) else pd.DataFrame(history)
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        curr_close = candle.get("close")
        if curr_close is None:
            return None

        # Need enough bars
        min_bars = max(self.vwap_lookback, self.rsi_period, self.ema_filter_period) + 5
        if len(history) < min_bars:
            return None

        # === VWAP PROXY (HLC3 weighted by volume) ===
        hlc3 = (history["high"] + history["low"] + history["close"]) / 3
        
        # Rolling VWAP over lookback window
        vwap_numerator = (hlc3 * history["volume"]).rolling(window=self.vwap_lookback).sum()
        vwap_denominator = history["volume"].rolling(window=self.vwap_lookback).sum()
        vwap = vwap_numerator / vwap_denominator
        
        curr_vwap = vwap.iloc[-1]
        
        # === RSI CALCULATION ===
        rsi_series = self._calculate_rsi(history["close"], self.rsi_period)
        curr_rsi = rsi_series.iloc[-1]
        
        # === EMA TREND FILTER ===
        ema_series = self._calculate_ema(history["close"], self.ema_filter_period)
        
        # Calculate EMA slope (trend strength)
        ema_slope = (ema_series.iloc[-1] - ema_series.iloc[-5]) / ema_series.iloc[-5] * 100
        trend_flat = abs(ema_slope) < 0.5  # Flat trend for mean reversion
        
        # === ATR VOLATILITY FILTER ===
        atr_series = self._calculate_atr(history, 14)
        curr_atr = atr_series.iloc[-1]
        atr_pct = (curr_atr / curr_close) * 100
        
        if atr_pct > self.max_atr_pct:
            return None  # Too volatile for mean reversion
        
        # === DEVIATION FROM VWAP ===
        deviation = ((curr_close - curr_vwap) / curr_vwap) * 100
        
        # Get current position state
        key = self._position_key(candle)
        position = self._positions.get(key)
        
        # === EXIT LOGIC (check first if we have a position) ===
        if position:
            position_side = position["side"]
            entry_idx = position.get("entry_bar_idx", 0)
            current_bar_idx = len(history) - 1
            position_age = current_bar_idx - entry_idx
            entry_deviation = position.get("entry_deviation", 0)
            entry_vwap = position.get("entry_vwap", curr_vwap)
            
            # Exit 1: VWAP Mean Reversion - price returned to VWAP
            if position_side == "long":
                if curr_close >= curr_vwap * (1 - self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=curr_close,
                        confidence=0.7,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "position_age": int(position_age),
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        }
                    )
            elif position_side == "short":
                if curr_close <= curr_vwap * (1 + self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=curr_close,
                        confidence=0.7,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "position_age": int(position_age),
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        }
                    )
            
            # Exit 2: Max Hold Time
            if position_age >= self.max_hold_candles:
                self._positions.pop(key, None)
                return Signal(
                    side="flat",
                    price=curr_close,
                    confidence=0.6,
                    meta={
                        "exit_reason": "max_hold_time",
                        "vwap": float(curr_vwap),
                        "close": float(curr_close),
                        "position_age": int(position_age),
                        "max_hold": self.max_hold_candles,
                        "strategy_type": "range_mean_reversion",
                        "version": "1.1.0",
                    }
                )
            
            # Exit 3: Stop Loss (if enabled)
            if self.stop_loss_enabled and entry_deviation is not None:
                if position_side == "long":
                    # For longs, we're below VWAP. If deviation increases, we're losing
                    if deviation <= -(abs(entry_deviation) * self.stop_loss_multiplier):
                        self._positions.pop(key, None)
                        return Signal(
                            side="flat",
                            price=curr_close,
                            confidence=0.65,
                            meta={
                                "exit_reason": "stop_loss",
                                "vwap": float(curr_vwap),
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            }
                        )
                elif position_side == "short":
                    # For shorts, we're above VWAP. If deviation decreases, we're losing
                    if deviation >= (abs(entry_deviation) * self.stop_loss_multiplier):
                        self._positions.pop(key, None)
                        return Signal(
                            side="flat",
                            price=curr_close,
                            confidence=0.65,
                            meta={
                                "exit_reason": "stop_loss",
                                "vwap": float(curr_vwap),
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            }
                        )
            
            return None

        # === ENTRY LOGIC (only if no position) ===
        # Long: Price below VWAP, RSI oversold, flat trend
        long_cond = (
            deviation < -self.deviation_pct and 
            curr_rsi < self.rsi_oversold and 
            trend_flat
        )
        
        # Short: Price above VWAP, RSI overbought, flat trend  
        short_cond = (
            deviation > self.deviation_pct and 
            curr_rsi > self.rsi_overbought and 
            trend_flat
        )

        if long_cond:
            current_bar_idx = len(history) - 1
            # Track position state
            self._positions[key] = {
                "side": "long",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_bar_idx": current_bar_idx,
                "entry_price": curr_close,
            }
            
            return Signal(
                side="long",
                price=curr_close,
                confidence=min(0.5 + abs(deviation) / 10, 0.9),
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "strategy_type": "range_mean_reversion",
                    "version": "1.1.0",
                    "exit_rules": {
                        "vwap_tolerance": self.vwap_tolerance,
                        "max_hold_candles": self.max_hold_candles,
                        "stop_loss_enabled": self.stop_loss_enabled,
                        "stop_loss_multiplier": self.stop_loss_multiplier,
                    },
                }
            )
        
        if short_cond:
            current_bar_idx = len(history) - 1
            # Track position state
            self._positions[key] = {
                "side": "short",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_bar_idx": current_bar_idx,
                "entry_price": curr_close,
            }
            
            return Signal(
                side="short",
                price=curr_close,
                confidence=min(0.5 + abs(deviation) / 10, 0.9),
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "strategy_type": "range_mean_reversion",
                    "version": "1.1.0",
                    "exit_rules": {
                        "vwap_tolerance": self.vwap_tolerance,
                        "max_hold_candles": self.max_hold_candles,
                        "stop_loss_enabled": self.stop_loss_enabled,
                        "stop_loss_multiplier": self.stop_loss_multiplier,
                    },
                }
            )

        return None
