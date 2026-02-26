"""Tests for StrategyEngine strategy loading and processing behavior."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from varon_fi import Signal
from signal_service.strategy.engine import StrategyEngine


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.last_query = None
        self.last_args = None

    async def fetch(self, query, *args):
        self.last_query = query
        self.last_args = args
        return self.rows


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


@pytest.mark.asyncio
async def test_load_strategies_from_db():
    rows = [
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "mtf_confluence",
            "type": "ta_lib",
            "params": {"htf_ema_len": 50},
            "symbols": ["BTC"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "live",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "11111111-1111-1111-1111-111111111111" in engine.strategies
    assert conn.last_args == ()
    assert "status = 'active'" in conn.last_query


class DummyStrategy:
    def __init__(self, name: str, side: str):
        self.name = name
        self.version = "1.0.0"
        self.symbols = ["BTC"]
        self.timeframes = ["5m"]
        self.params = {}
        self.mode = "paper"
        self._side = side

    def on_candle(self, _ohlc, _history):
        return Signal(side=self._side, price=50000.0, confidence=0.6)


@pytest.mark.asyncio
async def test_process_candle_signals_emits_all_matching_strategies(monkeypatch):
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine.strategies = {
        "s1": DummyStrategy("alpha", "long"),
        "s2": DummyStrategy("beta", "short"),
        "s3": DummyStrategy("gamma", "long"),
    }

    async def fake_fetch_history(*_args, **_kwargs):
        return pd.DataFrame(
            [{"timestamp": datetime.now(timezone.utc), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        )

    persisted = []

    async def fake_persist_signal(signal):
        persisted.append((signal.strategy_id, signal.side))
        return f"id-{signal.strategy_id}"

    monkeypatch.setattr(engine, "_fetch_history", fake_fetch_history)
    monkeypatch.setattr(engine, "_persist_signal", fake_persist_signal)

    candle = {
        "symbol": "BTC",
        "timeframe": "5m",
        "timestamp": datetime.now(timezone.utc),
        "open": 50000.0,
        "high": 50100.0,
        "low": 49900.0,
        "close": 50050.0,
        "volume": 10,
    }

    signals = await engine.process_candle_signals(candle)

    assert len(signals) == 3
    assert {s.strategy_id for s in signals} == {"s1", "s2", "s3"}
    assert persisted == [("s1", "long"), ("s2", "short"), ("s3", "long")]
    assert {s.strategy_id: s.signal_db_id for s in signals} == {
        "s1": "id-s1",
        "s2": "id-s2",
        "s3": "id-s3",
    }

    metrics = engine.get_metrics_snapshot()
    assert metrics["candles_processed"] == 1
    assert metrics["strategies_evaluated"] == 3
    assert metrics["signals_emitted"] == 3
    assert metrics["signals_dropped"] == 0


@pytest.mark.asyncio
async def test_process_candle_dedupes_per_strategy(monkeypatch):
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine.strategies = {
        "s1": DummyStrategy("alpha", "long"),
        "s2": DummyStrategy("beta", "short"),
    }

    async def fake_fetch_history(*_args, **_kwargs):
        return pd.DataFrame(
            [{"timestamp": datetime.now(timezone.utc), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        )

    persist_calls = 0

    async def fake_persist_signal(_signal):
        nonlocal persist_calls
        persist_calls += 1
        return f"id-{persist_calls}"

    monkeypatch.setattr(engine, "_fetch_history", fake_fetch_history)
    monkeypatch.setattr(engine, "_persist_signal", fake_persist_signal)

    ts = datetime.now(timezone.utc)
    candle = {
        "symbol": "BTC",
        "timeframe": "5m",
        "timestamp": ts,
        "open": 50000.0,
        "high": 50100.0,
        "low": 49900.0,
        "close": 50050.0,
        "volume": 10,
    }

    first = await engine.process_candle_signals(candle)
    second = await engine.process_candle_signals(candle)

    assert len(first) == 2
    assert second == []
    assert persist_calls == 2


@pytest.mark.asyncio
async def test_process_candle_wrapper_keeps_backward_compat(monkeypatch):
    engine = StrategyEngine("postgresql://localhost/varon_fi")

    async def fake_process_candle_signals(_ohlc):
        return [
            Signal(side="long", price=1.0, confidence=0.5),
            Signal(side="short", price=1.0, confidence=0.5),
        ]

    monkeypatch.setattr(engine, "process_candle_signals", fake_process_candle_signals)

    signal = await engine.process_candle({"symbol": "BTC", "timeframe": "5m"})
    assert signal is not None
    assert signal.side == "long"
