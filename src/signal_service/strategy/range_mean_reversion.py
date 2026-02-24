"""Range Mean Reversion Strategy with Exit Logic for live trading."""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register
from varon_fi.ta import atr, ema, rsi

logger = get_logger(__name__)


@register
class RangeMeanReversionStrategy(BaseStrategy):
    """
    Range Mean Reversion Scalper (VWAP Proxy) with Exit Logic

    Enters when price over-extends from short-term VWAP and
    RSI shows extreme conditions. Exits near VWAP, at max hold time,
    or via stop loss.

    Version: 1.1.0 (adds exits)
    """

    name = "range_mean_reversion"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.utc = timezone.utc

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

        # Track position state for exits (per symbol)
        self._positions: dict[str, dict] = {}

    def _position_key(self, candle: dict) -> str:
        """Generate position tracking key for symbol."""
        symbol = candle.get("symbol")
        if symbol:
            return symbol
        if self.symbols:
            return self.symbols[0]
        return "default"

    def _normalize_ts(self, ts) -> Optional[datetime]:
        """Normalize timestamps into timezone-aware UTC datetime."""
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, self.utc)
        elif hasattr(ts, "seconds") and hasattr(ts, "nanos"):
            dt = datetime.fromtimestamp(ts.seconds, self.utc)
        elif isinstance(ts, str):
            dt = pd.to_datetime(ts)
        elif isinstance(ts, datetime):
            dt = ts
        elif hasattr(ts, "ToDatetime"):
            dt = ts.ToDatetime(tzinfo=self.utc)
        else:
            return None

        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.utc)
        else:
            dt = dt.astimezone(self.utc)
        return dt

    def _timeframe_minutes(self, timeframe: Optional[str]) -> Optional[int]:
        """Convert a timeframe string like '5m' or '1h' into minutes."""
        if not timeframe:
            return None
        tf = str(timeframe).strip().lower()
        try:
            if tf.endswith("m"):
                minutes = int(tf[:-1])
                return minutes if minutes > 0 else None
            if tf.endswith("h"):
                hours = int(tf[:-1])
                return (hours * 60) if hours > 0 else None
            if tf.endswith("d"):
                days = int(tf[:-1]) if tf[:-1] else 1
                return (days * 24 * 60) if days > 0 else None
        except Exception:
            return None
        return None

    def _compute_vwap(self, history: pd.DataFrame) -> Optional[float]:
        """Compute rolling VWAP (HLC3 weighted by volume)."""
        if len(history) < self.vwap_lookback:
            return None

        hlc3 = (history["high"] + history["low"] + history["close"]) / 3
        vwap_num = (hlc3 * history["volume"]).rolling(window=self.vwap_lookback).sum()
        vwap_den = history["volume"].rolling(window=self.vwap_lookback).sum()
        vwap_series = vwap_num / vwap_den
        curr_vwap = vwap_series.iloc[-1]
        if pd.isna(curr_vwap):
            return None
        return float(curr_vwap)

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

        candle_ts = candle.get("timestamp") or candle.get("ts")
        ts = self._normalize_ts(candle_ts)

        key = self._position_key(candle)
        position = self._positions.get(key)

        # Compute VWAP if possible (used for both exits and entries)
        curr_vwap = self._compute_vwap(history)

        # === EXIT LOGIC (run even if history is short) ===
        if position:
            position_side = position.get("side")
            entry_vwap = position.get("entry_vwap")
            entry_deviation = position.get("entry_deviation")
            entry_ts = position.get("entry_ts")

            ref_vwap = curr_vwap or entry_vwap
            deviation = None
            if ref_vwap:
                deviation = ((curr_close - ref_vwap) / ref_vwap) * 100

            timeframe = candle.get("timeframe") or (self.timeframes[0] if self.timeframes else None)
            tf_minutes = self._timeframe_minutes(timeframe)
            position_age_minutes = None
            position_age_candles = None
            if ts and entry_ts:
                position_age_minutes = (ts - entry_ts).total_seconds() / 60
                if tf_minutes:
                    position_age_candles = position_age_minutes / tf_minutes

            # Exit 1: VWAP Mean Reversion - price returned to VWAP
            if ref_vwap:
                if position_side == "long" and curr_close >= ref_vwap * (1 - self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=curr_close,
                        confidence=0.7,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(ref_vwap),
                            "close": float(curr_close),
                            "position_age_minutes": position_age_minutes,
                            "position_age_candles": position_age_candles,
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        },
                    )
                if position_side == "short" and curr_close <= ref_vwap * (1 + self.vwap_tolerance):
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=curr_close,
                        confidence=0.7,
                        meta={
                            "exit_reason": "vwap_mean_reversion",
                            "vwap": float(ref_vwap),
                            "close": float(curr_close),
                            "position_age_minutes": position_age_minutes,
                            "position_age_candles": position_age_candles,
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        },
                    )

            # Exit 2: Max Hold Time (timestamp-based)
            if ts and entry_ts and tf_minutes and self.max_hold_candles:
                max_hold_minutes = self.max_hold_candles * tf_minutes
                if position_age_minutes is not None and position_age_minutes >= max_hold_minutes:
                    self._positions.pop(key, None)
                    return Signal(
                        side="flat",
                        price=curr_close,
                        confidence=0.6,
                        meta={
                            "exit_reason": "max_hold_time",
                            "vwap": float(ref_vwap) if ref_vwap else None,
                            "close": float(curr_close),
                            "position_age_minutes": position_age_minutes,
                            "position_age_candles": position_age_candles,
                            "max_hold": self.max_hold_candles,
                            "max_hold_minutes": max_hold_minutes,
                            "strategy_type": "range_mean_reversion",
                            "version": "1.1.0",
                        },
                    )

            # Exit 3: Stop Loss (if enabled)
            if self.stop_loss_enabled and deviation is not None and entry_deviation is not None:
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
                                "vwap": float(ref_vwap) if ref_vwap else None,
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            },
                        )
                elif position_side == "short":
                    # For shorts, we're above VWAP. If deviation increases, we're losing
                    if deviation >= (abs(entry_deviation) * self.stop_loss_multiplier):
                        self._positions.pop(key, None)
                        return Signal(
                            side="flat",
                            price=curr_close,
                            confidence=0.65,
                            meta={
                                "exit_reason": "stop_loss",
                                "vwap": float(ref_vwap) if ref_vwap else None,
                                "close": float(curr_close),
                                "deviation": float(deviation),
                                "entry_deviation": float(entry_deviation),
                                "strategy_type": "range_mean_reversion",
                                "version": "1.1.0",
                            },
                        )

            return None

        # === ENTRY LOGIC (only if no position) ===
        min_bars = max(self.vwap_lookback, self.rsi_period, self.ema_filter_period) + 5
        if len(history) < min_bars:
            return None

        if curr_vwap is None:
            return None

        closes = history["close"].values
        highs = history["high"].values
        lows = history["low"].values

        # RSI
        rsi_vals = rsi(closes, self.rsi_period)
        curr_rsi = rsi_vals[-1]

        # EMA trend filter
        ema_vals = ema(closes, self.ema_filter_period)
        if len(ema_vals) < 6 or pd.isna(ema_vals[-1]) or pd.isna(ema_vals[-5]):
            return None

        ema_slope = (ema_vals[-1] - ema_vals[-5]) / ema_vals[-5] * 100 if ema_vals[-5] else 0
        trend_flat = abs(ema_slope) < 0.5

        # ATR volatility filter
        atr_vals = atr(highs, lows, closes, 14)
        curr_atr = atr_vals[-1]
        if curr_atr is None or pd.isna(curr_atr):
            return None

        atr_pct = (curr_atr / curr_close) * 100 if curr_close else 0
        if atr_pct > self.max_atr_pct:
            return None

        # Deviation from VWAP
        deviation = ((curr_close - curr_vwap) / curr_vwap) * 100

        long_cond = deviation < -self.deviation_pct and curr_rsi < self.rsi_oversold and trend_flat
        short_cond = deviation > self.deviation_pct and curr_rsi > self.rsi_overbought and trend_flat

        if long_cond:
            self._positions[key] = {
                "side": "long",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_price": curr_close,
                "entry_ts": ts or datetime.now(self.utc),
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
                },
            )

        if short_cond:
            self._positions[key] = {
                "side": "short",
                "entry_vwap": float(curr_vwap),
                "entry_deviation": float(deviation),
                "entry_price": curr_close,
                "entry_ts": ts or datetime.now(self.utc),
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
                },
            )

        return None
