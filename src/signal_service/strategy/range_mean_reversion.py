"""Range Mean Reversion strategy wrapper using shared varon_fi logic."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from varon_fi import BaseStrategy, Signal, register
from varon_fi.strategy.range_mean_reversion_logic import (
    calculate_vwap,
    coerce_history_df,
    evaluate_entry,
    evaluate_exit,
    load_params,
)


@register
class RangeMeanReversionStrategy(BaseStrategy):
    """Live strategy wrapper around shared range mean reversion logic."""

    name = "range_mean_reversion"

    def __init__(self, params=None, **kwargs):
        raw_params = params or {}
        super().__init__(
            strategy_id=kwargs.get("strategy_id", ""),
            name=kwargs.get("name", self.name),
            version=kwargs.get("version", "1.1.0"),
            symbols=kwargs.get("symbols", []),
            timeframes=kwargs.get("timeframes", []),
            params=raw_params,
        )
        self._logic_params = load_params(raw_params)
        self._positions: dict[str, dict] = {}

    def _position_key(self, candle: dict) -> str:
        symbol = candle.get("symbol")
        if symbol:
            return str(symbol)
        if self.symbols:
            return str(self.symbols[0])
        return "default"

    def _normalize_ts(self, ts) -> Optional[datetime]:
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
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _timeframe_minutes(self, candle: dict) -> int:
        timeframe = str(candle.get("timeframe") or "5m").strip().lower()
        try:
            if timeframe.endswith("m"):
                return max(1, int(timeframe[:-1]))
            if timeframe.endswith("h"):
                return max(1, int(timeframe[:-1]) * 60)
            if timeframe.endswith("d"):
                return max(1, int(timeframe[:-1]) * 24 * 60)
        except Exception:
            return 5
        return 5

    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        try:
            history_df = coerce_history_df(history)
        except ValueError:
            return None
        if history_df.empty:
            return None

        close_value = candle.get("close")
        if close_value is None:
            close_value = history_df["close"].iloc[-1]
        curr_close = float(close_value)

        key = self._position_key(candle)
        position = self._positions.get(key)

        if position is not None:
            curr_vwap = calculate_vwap(history_df, self._logic_params.vwap_lookback)
            if curr_vwap is None:
                return None

            bars_held: Optional[int] = None
            entry_ts = position.get("entry_ts")
            candle_ts = self._normalize_ts(candle.get("timestamp") or candle.get("ts"))
            if entry_ts is not None and candle_ts is not None:
                elapsed_minutes = int((candle_ts - entry_ts).total_seconds() // 60)
                if elapsed_minutes >= 0:
                    bars_held = elapsed_minutes // self._timeframe_minutes(candle)

            decision = evaluate_exit(
                position_side=str(position.get("side", "flat")),
                curr_close=curr_close,
                curr_vwap=float(curr_vwap),
                entry_deviation=float(position.get("entry_deviation", 0.0)),
                params=self._logic_params,
                bars_held=bars_held,
            )
            if not decision:
                return None

            self._positions.pop(key, None)
            meta = dict(decision)
            meta.update({"vwap": float(curr_vwap), "close": curr_close})
            return Signal(side="flat", price=curr_close, confidence=0.6, meta=meta)

        decision = evaluate_entry(history_df, self._logic_params)
        if decision is None:
            return None

        side = str(decision.get("side", "flat"))
        if side not in {"long", "short"}:
            return None

        self._positions[key] = {
            "side": side,
            "entry_ts": self._normalize_ts(candle.get("timestamp") or candle.get("ts"))
            or datetime.now(timezone.utc),
            "entry_price": curr_close,
            "entry_deviation": float(decision.get("deviation_pct", 0.0)),
        }

        meta = {k: v for k, v in decision.items() if k != "side"}
        return Signal(side=side, price=curr_close, confidence=0.6, meta=meta)
