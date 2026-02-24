"""Breakout Retest strategy for live/paper trading."""

from typing import Optional

import pandas as pd
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register

logger = get_logger(__name__)


@register
class BreakoutRetestStrategy(BaseStrategy):
    """
    Breakout → Retest Scalper

    Waits for N-bar breakout, then enters on retest of breakout level
    with confirmation. Cleaner entries than chasing initial breakout.
    """

    name = "breakout_retest"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Track breakout state per symbol/timeframe
        self._state: dict[str, dict] = {}

    def _state_key(self, candle: dict) -> str:
        symbol = candle.get("symbol") or candle.get("instrument_id") or "unknown"
        timeframe = candle.get("timeframe") or "unknown"
        return f"{symbol}:{timeframe}"

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        if len(history) < 30:
            return None

        history = history.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        breakout_lookback = int(self.params.get("breakout_lookback", 20))
        max_retest_bars = int(self.params.get("max_retest_bars", 5))
        confirmation_close_pct = float(self.params.get("confirmation_close_pct", 0.3))
        volume_multiplier = float(self.params.get("volume_multiplier", 1.2))

        min_bars = breakout_lookback + max_retest_bars + 5
        if len(history) < min_bars:
            return None

        highs = history["high"].values
        lows = history["low"].values
        closes = history["close"].values
        volumes = history["volume"].values

        recent_highs = pd.Series(highs[-(breakout_lookback + 1):-1])
        recent_lows = pd.Series(lows[-(breakout_lookback + 1):-1])
        if recent_highs.empty or recent_lows.empty:
            return None

        breakout_level_high = recent_highs.max()
        breakout_level_low = recent_lows.min()

        curr_high = candle.get("high")
        curr_low = candle.get("low")
        curr_close = candle.get("close")
        curr_volume = candle.get("volume", 0)
        prev_close = closes[-2] if len(closes) > 1 else closes[-1]

        if curr_high is None or curr_low is None or curr_close is None:
            return None

        avg_volume = pd.Series(volumes).rolling(20).mean().values
        avg_vol = avg_volume[-1] if len(avg_volume) else 0
        volume_ok = curr_volume > (avg_vol * volume_multiplier) if avg_vol else True

        key = self._state_key(candle)
        state = self._state.setdefault(
            key,
            {"breakout_high": None, "breakout_low": None, "bars_since": 0},
        )

        fresh_breakout_up = (prev_close <= breakout_level_high) and (curr_close > breakout_level_high)
        fresh_breakout_down = (prev_close >= breakout_level_low) and (curr_close < breakout_level_low)

        if fresh_breakout_up and volume_ok:
            state["breakout_high"] = breakout_level_high
            state["breakout_low"] = None
            state["bars_since"] = 0
            return None

        if fresh_breakout_down and volume_ok:
            state["breakout_low"] = breakout_level_low
            state["breakout_high"] = None
            state["bars_since"] = 0
            return None

        if state["breakout_high"] is not None or state["breakout_low"] is not None:
            state["bars_since"] += 1

        if state["bars_since"] > max_retest_bars:
            state["breakout_high"] = None
            state["breakout_low"] = None
            state["bars_since"] = 0
            return None

        # Long: retest breakout high
        if state["breakout_high"] is not None:
            retest_zone_low = state["breakout_high"] * 0.998
            retest_zone_high = state["breakout_high"] * 1.002
            price_in_retest_zone = retest_zone_low <= curr_low <= retest_zone_high
            bullish_close = curr_close > (curr_low + (curr_high - curr_low) * confirmation_close_pct)

            if price_in_retest_zone and bullish_close and volume_ok:
                breakout_level = float(state["breakout_high"])
                state["breakout_high"] = None
                state["bars_since"] = 0
                return Signal(
                    side="long",
                    price=float(curr_close),
                    confidence=0.6,
                    meta={
                        "breakout_level": breakout_level,
                        "retest_low": float(curr_low),
                        "entry_close": float(curr_close),
                        "volume_ratio": float(curr_volume / avg_vol) if avg_vol else 0,
                    },
                )

        # Short: retest breakout low
        if state["breakout_low"] is not None:
            retest_zone_low = state["breakout_low"] * 0.998
            retest_zone_high = state["breakout_low"] * 1.002
            price_in_retest_zone = retest_zone_low <= curr_high <= retest_zone_high
            bearish_close = curr_close < (curr_high - (curr_high - curr_low) * confirmation_close_pct)

            if price_in_retest_zone and bearish_close and volume_ok:
                breakout_level = float(state["breakout_low"])
                state["breakout_low"] = None
                state["bars_since"] = 0
                return Signal(
                    side="short",
                    price=float(curr_close),
                    confidence=0.6,
                    meta={
                        "breakout_level": breakout_level,
                        "retest_high": float(curr_high),
                        "entry_close": float(curr_close),
                        "volume_ratio": float(curr_volume / avg_vol) if avg_vol else 0,
                    },
                )

        return None
