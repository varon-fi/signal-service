"""Range Mean Reversion strategy (VWAP proxy) for live/paper trading.

Version: 1.1.0 - Adds VWAP-based exit logic matching backtest implementation.
Exit conditions (priority order):
1. VWAP Mean Reversion - price returns to VWAP
2. Time-Based Exit - held too long without reversion
3. Stop Loss - price moves further against position
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register
from varon_fi.ta import ema, atr, rsi

logger = get_logger(__name__)


@register
class RangeMeanReversionStrategy(BaseStrategy):
    """
    Range Mean Reversion Scalper (VWAP Proxy)

    Enters when price over-extends from short-term VWAP and
    RSI shows extreme conditions. Exits near VWAP.
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

        # Exit parameters (v1.1.0)
        self.vwap_tolerance = float(self.params.get("vwap_tolerance", 0.002))
        self.max_hold_candles = int(self.params.get("max_hold_candles", 15))
        self.stop_loss_enabled = bool(self.params.get("stop_loss_enabled", True))
        self.stop_loss_multiplier = float(self.params.get("stop_loss_multiplier", 1.5))

        # Position tracking per symbol
        self._positions: dict[str, dict] = {}

    def _position_key(self, candle: dict) -> str:
        """Get position key from candle or default to first symbol."""
        symbol = candle.get("symbol")
        if symbol:
            return symbol
        if self.symbols:
            return self.symbols[0]
        return "default"

    def _calculate_vwap(self, history: pd.DataFrame) -> float:
        """Calculate current VWAP from history."""
        hlc3 = (history["high"] + history["low"] + history["close"]) / 3
        vwap_num = (hlc3 * history["volume"]).rolling(window=self.vwap_lookback).sum()
        vwap_den = history["volume"].rolling(window=self.vwap_lookback).sum()
        vwap_series = vwap_num / vwap_den
        return float(vwap_series.iloc[-1])

    def _normalize_ts(self, ts) -> Optional[datetime]:
        """Normalize timestamps into timezone-aware UTC datetime."""
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, timezone.utc)
        elif hasattr(ts, "seconds") and hasattr(ts, "nanos"):
            dt = datetime.fromtimestamp(ts.seconds, timezone.utc)
        elif isinstance(ts, str):
            dt = pd.to_datetime(ts)
        elif isinstance(ts, datetime):
            dt = ts
        else:
            return None

        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        # Convert Decimal columns to float
        history = history.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        curr_close = candle.get("close")
        if curr_close is None:
            return None

        closes = history["close"].values
        highs = history["high"].values
        lows = history["low"].values
        volumes = history["volume"].values

        # Calculate VWAP (needed for exits even with short history)
        try:
            curr_vwap = self._calculate_vwap(history)
        except Exception:
            curr_vwap = None

        # Calculate deviation for exits (doesn't require full history)
        deviation = ((curr_close - curr_vwap) / curr_vwap) * 100 if curr_vwap else 0

        # Get position key and check for existing position
        key = self._position_key(candle)
        position = self._positions.get(key)

        # === EXIT LOGIC ===
        if position:
            position_side = position["side"]
            entry_ts = position.get("entry_ts")
            entry_deviation = position.get("entry_deviation", 0)
            entry_price = position.get("entry_price", curr_close)
            entry_vwap = position.get("entry_vwap", curr_vwap)

            # Calculate position age
            position_age_candles = 0
            position_age_minutes = 0
            if entry_ts:
                candle_ts = self._normalize_ts(candle.get("timestamp") or candle.get("ts"))
                if candle_ts and entry_ts:
                    # Assume 5m candles by default; timeframe could be parameterized
                    timeframe_minutes = 5
                    if "timeframe" in candle and candle["timeframe"]:
                        tf = candle["timeframe"]
                        if tf.endswith("m"):
                            timeframe_minutes = int(tf[:-1])
                        elif tf.endswith("h"):
                            timeframe_minutes = int(tf[:-1]) * 60
                    position_age_minutes = int((candle_ts - entry_ts).total_seconds() / 60)
                    position_age_candles = position_age_minutes // timeframe_minutes

            # Exit 1: VWAP Mean Reversion (primary)
            if position_side == "long":
                if curr_close >= curr_vwap * (1 - self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=float(curr_close),
                        confidence=0.6,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "position_age_candles": position_age_candles,
                            "position_age_minutes": position_age_minutes,
                            "entry_price": float(entry_price),
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        },
                    )
            else:  # short
                if curr_close <= curr_vwap * (1 + self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=float(curr_close),
                        confidence=0.6,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "position_age_candles": position_age_candles,
                            "position_age_minutes": position_age_minutes,
                            "entry_price": float(entry_price),
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        },
                    )

            # Exit 2: Time-Based Exit (max hold candles)
            if position_age_candles >= self.max_hold_candles:
                self._positions.pop(key, None)
                return Signal(
                    side="flat",
                    price=float(curr_close),
                    confidence=0.55,
                    meta={
                        "exit_reason": "max_hold_time",
                        "vwap": float(curr_vwap),
                        "close": float(curr_close),
                        "position_age_candles": position_age_candles,
                        "position_age_minutes": position_age_minutes,
                        "max_hold": self.max_hold_candles,
                        "entry_price": float(entry_price),
                        "strategy_type": "range_mean_reversion",
                        "version": "1.1.0",
                    },
                )

            # Exit 3: Stop Loss
            if self.stop_loss_enabled and entry_deviation != 0:
                if position_side == "long":
                    # Long stop: deviation worsens beyond multiplier of entry deviation
                    if deviation <= -(abs(entry_deviation) * self.stop_loss_multiplier):
                        self._positions.pop(key, None)
                        return Signal(
                            side="flat",
                            price=float(curr_close),
                            confidence=0.55,
                            meta={
                                "exit_reason": "stop_loss",
                                "vwap": float(curr_vwap),
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "stop_loss_multiplier": self.stop_loss_multiplier,
                                "entry_price": float(entry_price),
                                "position_age_candles": position_age_candles,
                                "position_age_minutes": position_age_minutes,
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            },
                        )
                else:  # short
                    # Short stop: deviation worsens beyond multiplier of entry deviation
                    if deviation >= (abs(entry_deviation) * self.stop_loss_multiplier):
                        self._positions.pop(key, None)
                        return Signal(
                            side="flat",
                            price=float(curr_close),
                            confidence=0.55,
                            meta={
                                "exit_reason": "stop_loss",
                                "vwap": float(curr_vwap),
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "stop_loss_multiplier": self.stop_loss_multiplier,
                                "entry_price": float(entry_price),
                                "position_age_candles": position_age_candles,
                                "position_age_minutes": position_age_minutes,
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            },
                        )

            # No exit condition met, stay in position
            return None

        # === ENTRY LOGIC (only when flat) ===
        # Entry requires sufficient history for technical indicators
        min_bars = max(self.vwap_lookback, self.rsi_period, self.ema_filter_period) + 5
        if len(history) < min_bars:
            return None

        # RSI calculation
        rsi_vals = rsi(closes, self.rsi_period)
        curr_rsi = rsi_vals[-1]

        # EMA trend filter
        ema_vals = ema(closes, self.ema_filter_period)
        if len(ema_vals) < 6 or pd.isna(ema_vals[-1]):
            return None

        ema_slope = (ema_vals[-1] - ema_vals[-5]) / ema_vals[-5] * 100 if ema_vals[-5] else 0
        trend_flat = abs(ema_slope) < 0.5

        # ATR volatility filter
        atr_vals = atr(highs, lows, closes, 14)
        curr_atr = atr_vals[-1]
        atr_pct = (curr_atr / curr_close) * 100 if curr_close else 0
        if atr_pct > self.max_atr_pct:
            return None

        long_cond = (
            deviation < -self.deviation_pct and
            curr_rsi < self.rsi_oversold and
            trend_flat
        )

        short_cond = (
            deviation > self.deviation_pct and
            curr_rsi > self.rsi_overbought and
            trend_flat
        )

        ts = self._normalize_ts(candle.get("timestamp") or candle.get("ts"))

        if long_cond:
            self._positions[key] = {
                "side": "long",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_ts": ts or datetime.now(timezone.utc),
                "entry_price": float(curr_close),
            }
            return Signal(
                side="long",
                price=float(curr_close),
                confidence=0.6,
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "exit_rules": {
                        "vwap_tolerance": self.vwap_tolerance,
                        "max_hold_candles": self.max_hold_candles,
                        "stop_loss_enabled": self.stop_loss_enabled,
                        "stop_loss_multiplier": self.stop_loss_multiplier,
                    },
                },
            )

        if short_cond:
            self._positions[key] = {
                "side": "short",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_ts": ts or datetime.now(timezone.utc),
                "entry_price": float(curr_close),
            }
            return Signal(
                side="short",
                price=float(curr_close),
                confidence=0.6,
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "exit_rules": {
                        "vwap_tolerance": self.vwap_tolerance,
                        "max_hold_candles": self.max_hold_candles,
                        "stop_loss_enabled": self.stop_loss_enabled,
                        "stop_loss_multiplier": self.stop_loss_multiplier,
                    },
                },
            )

        return None
