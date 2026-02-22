"""Liquidity Sweep Reversal strategy for live/paper trading."""

from typing import Optional

import pandas as pd
from structlog import get_logger

from varon_fi import BaseStrategy, Signal, register
from varon_fi.ta import atr

logger = get_logger(__name__)


@register
class LiquiditySweepReversalStrategy(BaseStrategy):
    """
    Liquidity Sweep Reversal Scalper

    Detects price sweeps beyond swing high/low (stop hunting),
    then enters when price snaps back and closes inside range.
    """

    name = "liquidity_sweep_reversal"

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        if len(history) < 30:
            return None

        # Convert Decimal columns to float
        history = history.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        swing_lookback = int(self.params.get("swing_lookback", 20))
        volume_multiplier = float(self.params.get("volume_multiplier", 1.5))
        min_atr = float(self.params.get("min_atr", 0.0))

        min_bars = swing_lookback + 5
        if len(history) < min_bars:
            return None

        highs = history["high"].values
        lows = history["low"].values
        closes = history["close"].values
        volumes = history["volume"].values

        recent_highs = pd.Series(highs[-(swing_lookback + 1):-1])
        recent_lows = pd.Series(lows[-(swing_lookback + 1):-1])
        if recent_highs.empty or recent_lows.empty:
            return None

        swing_high = recent_highs.max()
        swing_low = recent_lows.min()

        curr_high = candle.get("high")
        curr_low = candle.get("low")
        curr_close = candle.get("close")
        curr_volume = candle.get("volume", 0)

        if curr_high is None or curr_low is None or curr_close is None:
            return None

        atr_vals = atr(highs, lows, closes, 14)
        curr_atr = atr_vals[-1]
        if curr_atr < min_atr:
            return None

        avg_volume = pd.Series(volumes).rolling(20).mean().values
        avg_vol = avg_volume[-1] if len(avg_volume) else 0
        volume_ok = curr_volume > (avg_vol * volume_multiplier) if avg_vol else True
        if not volume_ok:
            return None

        swept_high = (curr_high > swing_high) and (curr_close < swing_high)
        swept_low = (curr_low < swing_low) and (curr_close > swing_low)

        if swept_high:
            return Signal(
                side="short",
                price=float(curr_close),
                confidence=0.65,
                meta={
                    "swing_high": float(swing_high),
                    "wick_high": float(curr_high),
                    "close": float(curr_close),
                    "atr": float(curr_atr),
                    "volume_ratio": float(curr_volume / avg_vol) if avg_vol else 0,
                },
            )

        if swept_low:
            return Signal(
                side="long",
                price=float(curr_close),
                confidence=0.65,
                meta={
                    "swing_low": float(swing_low),
                    "wick_low": float(curr_low),
                    "close": float(curr_close),
                    "atr": float(curr_atr),
                    "volume_ratio": float(curr_volume / avg_vol) if avg_vol else 0,
                },
            )

        return None
