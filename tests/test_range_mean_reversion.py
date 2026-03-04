"""Signal-service wrapper tests for shared range mean reversion logic."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from signal_service.strategy.range_mean_reversion import RangeMeanReversionStrategy


def _history() -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(30):
        px = 100.0 + (i * 0.1)
        rows.append(
            {
                "timestamp": now,
                "open": px,
                "high": px + 0.2,
                "low": px - 0.2,
                "close": px,
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows)


def _strategy() -> RangeMeanReversionStrategy:
    return RangeMeanReversionStrategy(
        strategy_id="test-123",
        name="range_mean_reversion",
        version="1.1.0",
        symbols=["BTC"],
        timeframes=["5m"],
        params={
            "vwap_lookback": 20,
            "rsi_period": 14,
            "ema_filter_period": 50,
            "deviation_pct": 1.0,
            "vwap_tolerance": 0.002,
            "max_hold_candles": 15,
            "stop_loss_enabled": True,
            "stop_loss_multiplier": 1.5,
        },
    )


def test_entry_signal_uses_shared_decision(monkeypatch):
    strategy = _strategy()

    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.evaluate_entry",
        lambda _history, _params: {
            "side": "long",
            "vwap": 100.0,
            "deviation_pct": -1.2,
            "rsi": 28.0,
            "atr_pct": 0.3,
        },
    )

    candle = {
        "timestamp": datetime.now(timezone.utc),
        "symbol": "BTC",
        "timeframe": "5m",
        "open": 99.0,
        "high": 99.2,
        "low": 98.8,
        "close": 99.0,
        "volume": 1200.0,
    }

    signal = strategy.on_candle(candle, _history())

    assert signal is not None
    assert signal.side == "long"
    assert signal.meta["vwap"] == 100.0
    assert strategy._positions["BTC"]["side"] == "long"


def test_exit_signal_uses_shared_decision(monkeypatch):
    strategy = _strategy()
    strategy._positions["BTC"] = {
        "side": "long",
        "entry_ts": datetime.now(timezone.utc),
        "entry_price": 99.0,
        "entry_deviation": -1.2,
    }

    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.calculate_vwap",
        lambda _history, _lookback: 100.0,
    )
    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.evaluate_exit",
        lambda **_kwargs: {"reason": "vwap_mean_reversion"},
    )

    candle = {
        "timestamp": datetime.now(timezone.utc),
        "symbol": "BTC",
        "timeframe": "5m",
        "open": 100.0,
        "high": 100.2,
        "low": 99.8,
        "close": 100.1,
        "volume": 1200.0,
    }

    signal = strategy.on_candle(candle, _history())

    assert signal is not None
    assert signal.side == "flat"
    assert signal.meta["reason"] == "vwap_mean_reversion"
    assert "BTC" not in strategy._positions


def test_open_position_holds_when_no_exit(monkeypatch):
    strategy = _strategy()
    strategy._positions["BTC"] = {
        "side": "short",
        "entry_ts": datetime.now(timezone.utc),
        "entry_price": 101.0,
        "entry_deviation": 1.2,
    }

    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.calculate_vwap",
        lambda _history, _lookback: 100.0,
    )
    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.evaluate_exit",
        lambda **_kwargs: None,
    )

    candle = {
        "timestamp": datetime.now(timezone.utc),
        "symbol": "BTC",
        "timeframe": "5m",
        "open": 101.0,
        "high": 101.3,
        "low": 100.8,
        "close": 101.1,
        "volume": 1000.0,
    }

    signal = strategy.on_candle(candle, _history())

    assert signal is None
    assert strategy._positions["BTC"]["side"] == "short"


def test_invalid_entry_side_is_ignored(monkeypatch):
    strategy = _strategy()

    monkeypatch.setattr(
        "signal_service.strategy.range_mean_reversion.evaluate_entry",
        lambda _history, _params: {"side": "flat", "vwap": 100.0},
    )

    candle = {
        "timestamp": datetime.now(timezone.utc),
        "symbol": "BTC",
        "timeframe": "5m",
        "open": 100.0,
        "high": 100.2,
        "low": 99.8,
        "close": 100.0,
        "volume": 1000.0,
    }

    signal = strategy.on_candle(candle, _history())

    assert signal is None
    assert strategy._positions == {}
