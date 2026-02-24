"""Range Mean Reversion strategy (VWAP proxy) with exit logic for live/paper trading."""

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
    Range Mean Reversion Scalper (VWAP Proxy) with Exit Logic

    Enters when price over-extends from short-term VWAP and
    RSI shows extreme conditions. Exits near VWAP, at max hold time,
    or via stop loss.

    Based on: docs/research/scalping-strategies.md #2
    Version: 1.1.0 (adds exits)
    """

    name = "range_mean_reversion"
    version = "1.1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Track position state for exits
        self._position = None
        self._entry_price = None
        self._entry_vwap = None
        self._entry_deviation = None
        self._entry_timestamp = None

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        # Convert Decimal columns to float
        history = history.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        # Get current timestamp
        current_ts = candle.get("timestamp") or candle.get("ts") or datetime.now(timezone.utc)
        if isinstance(current_ts, str):
            current_ts = pd.to_datetime(current_ts)

        curr_close = candle.get("close")
        if curr_close is None:
            return None

        # Calculate VWAP (even with minimal history for exits)
        vwap_lookback = int(self.params.get("vwap_lookback", 20))
        min_vwap_bars = min(vwap_lookback, len(history))

        if len(history) >= min_vwap_bars and min_vwap_bars > 0:
            hlc3 = (history["high"].iloc[-min_vwap_bars:] +
                    history["low"].iloc[-min_vwap_bars:] +
                    history["close"].iloc[-min_vwap_bars:]) / 3
            vwap_num = (hlc3 * history["volume"].iloc[-min_vwap_bars:]).sum()
            vwap_den = history["volume"].iloc[-min_vwap_bars:].sum()
            curr_vwap = vwap_num / vwap_den if vwap_den > 0 else curr_close
        else:
            # Not enough history for VWAP, use close as fallback for exits
            curr_vwap = curr_close

        # === EXIT LOGIC (runs even with limited history) ===
        if self._position is not None and self._position != 0:
            exit_signal = self._check_exits(
                curr_close=curr_close,
                curr_vwap=curr_vwap,
                current_ts=current_ts
            )
            if exit_signal:
                self._reset_position()
                return exit_signal

        # === ENTRY LOGIC (requires sufficient history) ===
        rsi_period = int(self.params.get("rsi_period", 14))
        ema_filter_period = int(self.params.get("ema_filter_period", 50))
        min_bars = max(vwap_lookback, rsi_period, ema_filter_period) + 5

        if len(history) < min_bars:
            return None

        rsi_oversold = float(self.params.get("rsi_oversold", 30))
        rsi_overbought = float(self.params.get("rsi_overbought", 70))
        deviation_pct = float(self.params.get("deviation_pct", 1.0))
        max_atr_pct = float(self.params.get("max_atr_pct", 2.0))

        closes = history["close"].values
        highs = history["high"].values
        lows = history["low"].values

        # RSI
        rsi_vals = rsi(closes, rsi_period)
        curr_rsi = rsi_vals[-1]

        # EMA trend filter
        ema_vals = ema(closes, ema_filter_period)
        if len(ema_vals) < 6 or pd.isna(ema_vals[-1]):
            return None

        ema_slope = (ema_vals[-1] - ema_vals[-5]) / ema_vals[-5] * 100 if ema_vals[-5] else 0
        trend_flat = abs(ema_slope) < 0.5

        # ATR volatility filter
        atr_vals = atr(highs, lows, closes, 14)
        curr_atr = atr_vals[-1]
        atr_pct = (curr_atr / curr_close) * 100 if curr_close else 0
        if atr_pct > max_atr_pct:
            return None

        # Deviation from VWAP
        deviation = ((curr_close - curr_vwap) / curr_vwap) * 100 if curr_vwap else 0

        long_cond = deviation < -deviation_pct and curr_rsi < rsi_oversold and trend_flat
        short_cond = deviation > deviation_pct and curr_rsi > rsi_overbought and trend_flat

        if long_cond:
            self._set_position("long", curr_close, curr_vwap, deviation, current_ts)
            return Signal(
                side="long",
                price=float(curr_close),
                confidence=min(0.5 + abs(deviation) / 10, 0.9),
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "strategy_type": "range_mean_reversion",
                    "version": "1.1.0",
                    "exit_rules": {
                        "vwap_tolerance": self.params.get("vwap_tolerance", 0.002),
                        "max_hold_minutes": self.params.get("max_hold_minutes", 75),
                        "stop_loss_enabled": self.params.get("stop_loss_enabled", True),
                        "stop_loss_multiplier": self.params.get("stop_loss_multiplier", 1.5),
                    },
                }
            )

        if short_cond:
            self._set_position("short", curr_close, curr_vwap, deviation, current_ts)
            return Signal(
                side="short",
                price=float(curr_close),
                confidence=min(0.5 + abs(deviation) / 10, 0.9),
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                    "strategy_type": "range_mean_reversion",
                    "version": "1.1.0",
                    "exit_rules": {
                        "vwap_tolerance": self.params.get("vwap_tolerance", 0.002),
                        "max_hold_minutes": self.params.get("max_hold_minutes", 75),
                        "stop_loss_enabled": self.params.get("stop_loss_enabled", True),
                        "stop_loss_multiplier": self.params.get("stop_loss_multiplier", 1.5),
                    },
                }
            )

        return None

    def _set_position(self, side: str, entry_price: float, entry_vwap: float,
                      entry_deviation: float, entry_ts: datetime):
        """Track position state for exits."""
        self._position = 1 if side == "long" else -1
        self._entry_price = entry_price
        self._entry_vwap = entry_vwap
        self._entry_deviation = entry_deviation
        self._entry_timestamp = entry_ts

    def _reset_position(self):
        """Clear position state after exit."""
        self._position = None
        self._entry_price = None
        self._entry_vwap = None
        self._entry_deviation = None
        self._entry_timestamp = None

    def _check_exits(self, curr_close: float, curr_vwap: float,
                     current_ts: datetime) -> Optional[Signal]:
        """
        Check all exit conditions:
        1. VWAP mean reversion (price returns to VWAP)
        2. Time-based max hold (timestamp-based)
        3. Stop loss (based on deviation multiplier)
        """
        if self._position is None or self._position == 0:
            return None

        position_side = "long" if self._position > 0 else "short"

        # Exit parameters
        vwap_tolerance = float(self.params.get("vwap_tolerance", 0.002))
        max_hold_minutes = int(self.params.get("max_hold_minutes", 75))  # Timestamp-based
        stop_loss_enabled = bool(self.params.get("stop_loss_enabled", True))
        stop_loss_multiplier = float(self.params.get("stop_loss_multiplier", 1.5))

        # Exit 1: VWAP Mean Reversion
        if position_side == "long":
            if curr_close >= curr_vwap * (1 - vwap_tolerance):
                return Signal(
                    side="flat",
                    price=float(curr_close),
                    confidence=0.7,
                    meta={
                        "exit_reason": "vwap_mean_reversion",
                        "vwap": float(curr_vwap),
                        "close": float(curr_close),
                    }
                )
        else:  # short
            if curr_close <= curr_vwap * (1 + vwap_tolerance):
                return Signal(
                    side="flat",
                    price=float(curr_close),
                    confidence=0.7,
                    meta={
                        "exit_reason": "vwap_mean_reversion",
                        "vwap": float(curr_vwap),
                        "close": float(curr_close),
                    }
                )

        # Exit 2: Time-based Max Hold (using timestamp, not bar index)
        if self._entry_timestamp is not None and current_ts is not None:
            try:
                hold_duration = (current_ts - self._entry_timestamp).total_seconds() / 60
                if hold_duration >= max_hold_minutes:
                    return Signal(
                        side="flat",
                        price=float(curr_close),
                        confidence=0.6,
                        meta={
                            "exit_reason": "max_hold_time",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "hold_minutes": float(hold_duration),
                            "max_hold_minutes": max_hold_minutes,
                        }
                    )
            except (TypeError, AttributeError):
                pass  # If timestamps aren't comparable, skip time-based exit

        # Exit 3: Stop Loss (based on deviation)
        if stop_loss_enabled and self._entry_deviation is not None:
            current_deviation = ((curr_close - curr_vwap) / curr_vwap) * 100 if curr_vwap else 0

            if position_side == "long":
                # Longs entered below VWAP (negative deviation)
                # Exit if deviation worsens by multiplier
                stop_threshold = -abs(self._entry_deviation) * stop_loss_multiplier
                if current_deviation <= stop_threshold:
                    return Signal(
                        side="flat",
                        price=float(curr_close),
                        confidence=0.8,
                        meta={
                            "exit_reason": "stop_loss",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "current_deviation": float(current_deviation),
                            "entry_deviation": float(self._entry_deviation),
                            "stop_threshold": float(stop_threshold),
                        }
                    )
            else:  # short
                # Shorts entered above VWAP (positive deviation)
                stop_threshold = abs(self._entry_deviation) * stop_loss_multiplier
                if current_deviation >= stop_threshold:
                    return Signal(
                        side="flat",
                        price=float(curr_close),
                        confidence=0.8,
                        meta={
                            "exit_reason": "stop_loss",
                            "vwap": float(curr_vwap),
                            "close": float(curr_close),
                            "current_deviation": float(current_deviation),
                            "entry_deviation": float(self._entry_deviation),
                            "stop_threshold": float(stop_threshold),
                        }
                    )

        return None
