"""Low Volatility Momentum Strategy implementation for live trading."""

from datetime import datetime, time
from typing import Optional

import pandas as pd
import pytz
import numpy as np
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class LowVolMomentumStrategy(BaseStrategy):
    """
    Low Volatility Momentum Strategy - Live Version

    **Matching ad‑hoc logic**:
    - Enter only in low‑vol regime (ATR percentile < threshold)
    - Momentum signal only (no candle confirmation by default)
    - Exit on: stop loss %, regime change, or max hold hours
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
        self.max_hold_hours = int(self.params.get("max_hold_hours", 48))
        self.exit_on_regime_change = bool(self.params.get("exit_on_regime_change", True))
        self.require_candle_confirmation = bool(self.params.get("require_candle_confirmation", False))

        # Session times (optional)
        session_start = self.params.get("session_start")
        session_end = self.params.get("session_end")
        self.session_start = self._parse_time(session_start) if session_start else None
        self.session_end = self._parse_time(session_end) if session_end else None

        # In‑memory position state (per symbol)
        self._positions: dict[str, dict] = {}

    def _parse_time(self, time_str: str) -> time:
        """Parse time string to time object."""
        if isinstance(time_str, str):
            return datetime.strptime(time_str, "%H:%M").time()
        return time_str

    def _normalize_ts(self, ts) -> Optional[datetime]:
        """Normalize timestamps into timezone‑aware UTC datetime."""
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
            dt = self.utc.localize(dt)
        else:
            dt = dt.astimezone(self.utc)
        return dt

    def _in_session(self, ts) -> bool:
        """Check if timestamp is within trading session."""
        if self.session_start is None or self.session_end is None:
            return True

        ts = self._normalize_ts(ts)
        if not isinstance(ts, datetime):
            return True

        current_time = ts.time()
        return self.session_start <= current_time <= self.session_end

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
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
            return "unknown", 50.0

        # Calculate ATR
        atr = self._calculate_atr(history, self.atr_period)
        atr_pct = (atr / history["close"]) * 100

        # Require full lookback window for percentile (match ad‑hoc)
        lookback_periods = self.lookback_days * 24 * 4  # 15m candles
        if len(atr_pct) < lookback_periods:
            return "unknown", 50.0

        # Calculate percentile of current ATR
        current_atr_pct = atr_pct.iloc[-1]
        atr_history = atr_pct.iloc[-lookback_periods:].dropna()

        if len(atr_history) < 10:
            return "unknown", 50.0

        percentile = (atr_history < current_atr_pct).mean() * 100

        # Classify regime
        if percentile < self.low_vol_threshold:
            return "low", percentile
        elif percentile > 70:
            return "high", percentile
        else:
            return "mid", percentile

    def _position_key(self, candle: dict) -> str:
        symbol = candle.get("symbol")
        if symbol:
            return symbol
        if self.symbols:
            return self.symbols[0]
        return "default"

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process new candle and return signal if conditions met."""
        # Convert Decimal columns to float
        history = history.copy() if isinstance(history, pd.DataFrame) else pd.DataFrame(history)
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        curr_close = candle.get("close")
        curr_open = candle.get("open")
        if curr_close is None:
            return None

        # Session filter
        candle_ts = candle.get("timestamp") or candle.get("ts")
        if candle_ts is not None and not self._in_session(candle_ts):
            return None

        ts = self._normalize_ts(candle_ts)
        key = self._position_key(candle)
        position = self._positions.get(key)

        # Exit logic (if in position)
        if position:
            side = position["side"]
            entry_price = position["entry_price"]
            entry_ts = position.get("entry_ts")
            entry_regime = position.get("entry_regime", "low")

            # Stop loss check
            pnl_pct = ((curr_close - entry_price) / entry_price) * 100 if side == "long" else ((entry_price - curr_close) / entry_price) * 100
            if pnl_pct <= -self.stop_loss_pct:
                self._positions.pop(key, None)
                return Signal(
                    side="short" if side == "long" else "long",
                    price=curr_close,
                    confidence=0.6,
                    meta={
                        "exit_reason": "stop_loss",
                        "entry_price": entry_price,
                        "pnl_pct": float(pnl_pct),
                        "strategy_type": "low_vol_momentum",
                    },
                )

            # Max hold check
            if ts and entry_ts and self.max_hold_hours is not None:
                held_hours = (ts - entry_ts).total_seconds() / 3600
                if held_hours >= self.max_hold_hours:
                    self._positions.pop(key, None)
                    return Signal(
                        side="short" if side == "long" else "long",
                        price=curr_close,
                        confidence=0.55,
                        meta={
                            "exit_reason": "max_hold",
                            "held_hours": float(held_hours),
                            "entry_price": entry_price,
                            "pnl_pct": float(pnl_pct),
                            "strategy_type": "low_vol_momentum",
                        },
                    )

            # Regime change check
            if self.exit_on_regime_change:
                regime, atr_percentile = self._get_vol_regime(history)
                if regime not in ("unknown", entry_regime):
                    self._positions.pop(key, None)
                    return Signal(
                        side="short" if side == "long" else "long",
                        price=curr_close,
                        confidence=0.55,
                        meta={
                            "exit_reason": "regime_change",
                            "atr_percentile": float(atr_percentile),
                            "entry_price": entry_price,
                            "pnl_pct": float(pnl_pct),
                            "strategy_type": "low_vol_momentum",
                        },
                    )

            return None

        # Entry logic (only when flat)
        if len(history) < self.atr_period * 2:
            return None

        regime, atr_percentile = self._get_vol_regime(history)
        if regime != "low":
            return None

        momentum_periods = self.momentum_lookback * 4  # 15m candles
        if len(history) < momentum_periods:
            return None

        momentum = (history["close"].iloc[-1] - history["close"].iloc[-momentum_periods]) / history["close"].iloc[-momentum_periods]

        long_cond = momentum > 0.01
        short_cond = momentum < -0.01

        if self.require_candle_confirmation:
            long_cond = long_cond and (curr_close > (curr_open or curr_close))
            short_cond = short_cond and (curr_close < (curr_open or curr_close))

        if long_cond or short_cond:
            side = "long" if long_cond else "short"
            self._positions[key] = {
                "side": side,
                "entry_price": curr_close,
                "entry_ts": ts or datetime.now(self.utc),
                "entry_regime": regime,
            }
            return Signal(
                side=side,
                price=curr_close,
                confidence=min(0.5 + abs(momentum) * 10, 0.9),
                meta={
                    "momentum": float(momentum),
                    "atr_percentile": float(atr_percentile),
                    "regime": regime,
                    "momentum_lookback_hours": self.momentum_lookback,
                    "strategy_type": "low_vol_momentum",
                    "exit_rules": {
                        "stop_loss_pct": self.stop_loss_pct,
                        "max_hold_hours": self.max_hold_hours,
                        "exit_on_regime_change": self.exit_on_regime_change,
                    },
                },
            )

        return None
