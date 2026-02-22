"""Range Mean Reversion strategy (VWAP proxy) for live/paper trading."""

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

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        if len(history) < 50:
            return None

        # Convert Decimal columns to float
        history = history.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in history.columns:
                history[col] = history[col].astype(float)

        numeric_fields = {"open", "high", "low", "close", "volume", "price"}
        candle = {k: float(v) if k in numeric_fields and v is not None else v for k, v in candle.items()}

        # Parameters
        vwap_lookback = int(self.params.get("vwap_lookback", 20))
        rsi_period = int(self.params.get("rsi_period", 14))
        rsi_oversold = float(self.params.get("rsi_oversold", 30))
        rsi_overbought = float(self.params.get("rsi_overbought", 70))
        deviation_pct = float(self.params.get("deviation_pct", 1.0))
        ema_filter_period = int(self.params.get("ema_filter_period", 50))
        max_atr_pct = float(self.params.get("max_atr_pct", 2.0))

        min_bars = max(vwap_lookback, rsi_period, ema_filter_period) + 5
        if len(history) < min_bars:
            return None

        closes = history["close"].values
        highs = history["high"].values
        lows = history["low"].values
        volumes = history["volume"].values

        # VWAP proxy (HLC3 weighted by volume)
        hlc3 = (history["high"] + history["low"] + history["close"]) / 3
        vwap_num = (hlc3 * history["volume"]).rolling(window=vwap_lookback).sum()
        vwap_den = history["volume"].rolling(window=vwap_lookback).sum()
        vwap_series = vwap_num / vwap_den

        curr_vwap = vwap_series.iloc[-1]
        curr_close = candle.get("close")

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
        if curr_close is None or curr_vwap is None:
            return None

        atr_pct = (curr_atr / curr_close) * 100 if curr_close else 0
        if atr_pct > max_atr_pct:
            return None

        # Deviation from VWAP
        deviation = ((curr_close - curr_vwap) / curr_vwap) * 100 if curr_vwap else 0

        long_cond = deviation < -deviation_pct and curr_rsi < rsi_oversold and trend_flat
        short_cond = deviation > deviation_pct and curr_rsi > rsi_overbought and trend_flat

        if long_cond:
            return Signal(
                side="long",
                price=float(curr_close),
                confidence=0.6,
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                },
            )

        if short_cond:
            return Signal(
                side="short",
                price=float(curr_close),
                confidence=0.6,
                meta={
                    "vwap": float(curr_vwap),
                    "deviation_pct": float(deviation),
                    "rsi": float(curr_rsi),
                    "atr_pct": float(atr_pct),
                },
            )

        return None
