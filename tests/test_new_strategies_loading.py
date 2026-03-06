"""Tests for strict strategy loading in signal-service."""

import pytest

from signal_service.strategy.engine import StrategyEngine


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.args = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
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
async def test_load_range_mean_reversion_only():
    rows = [
        {
            "id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "name": "range_mean_reversion",
            "type": "ta_lib",
            "params": '{"vwap_lookback": 20, "rsi_period": 14}',
            "symbol": "BTC",
            "timeframe": "5m",
            "meta": {"strategy_params": {"vwap_lookback": 28}},
            "version": "1.1.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "ffffffff-ffff-ffff-ffff-ffffffffffff:BTC:5m" in engine.strategies
    strategy = engine.strategies["ffffffff-ffff-ffff-ffff-ffffffffffff:BTC:5m"]
    assert strategy.name == "range_mean_reversion"
    assert strategy.params["vwap_lookback"] == 28
    assert strategy.params["rsi_period"] == 14


@pytest.mark.asyncio
async def test_skip_unsupported_strategy_names():
    rows = [
        {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "legacy_strategy",
            "type": "ta_lib",
            "params": '{}',
            "symbol": "BTC",
            "timeframe": "5m",
            "meta": {},
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
        {
            "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "name": "range_mean_reversion",
            "type": "ta_lib",
            "params": '{}',
            "symbol": "BTC",
            "timeframe": "5m",
            "meta": {},
            "version": "1.1.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
    ]

    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    loaded_ids = set(engine.strategies.keys())
    assert loaded_ids == {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:BTC:5m"}
