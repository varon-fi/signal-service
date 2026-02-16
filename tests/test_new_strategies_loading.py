"""Test that new strategies can be loaded by StrategyEngine."""

import pytest
import asyncio
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
async def test_load_volatility_expansion():
    """Test loading volatility_expansion strategy."""
    rows = [
        {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "volatility_expansion",
            "type": "ta_lib",
            "params": '{"keltner_len": 20, "atr_mult": 2.0, "bb_len": 20}',
            "symbols": ["BTC", "ETH"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="paper")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" in engine.strategies
    strategy = engine.strategies["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]
    assert strategy.name == "volatility_expansion"
    assert strategy.params["keltner_len"] == 20


@pytest.mark.asyncio
async def test_load_volume_range_breakout():
    """Test loading volume_range_breakout strategy."""
    rows = [
        {
            "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "name": "volume_range_breakout",
            "type": "ta_lib",
            "params": '{"lookback": 20, "volume_threshold": 1.5}',
            "symbols": ["BTC", "ETH"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="paper")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" in engine.strategies
    strategy = engine.strategies["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]
    assert strategy.name == "volume_range_breakout"
    assert strategy.params["lookback"] == 20


@pytest.mark.asyncio
async def test_load_momentum():
    """Test loading momentum strategy."""
    rows = [
        {
            "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "name": "momentum",
            "type": "ta_lib",
            "params": '{"rsi_length": 14, "rsi_overbought": 65}',
            "symbols": ["BTC", "ETH"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="paper")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "cccccccc-cccc-cccc-cccc-cccccccccccc" in engine.strategies
    strategy = engine.strategies["cccccccc-cccc-cccc-cccc-cccccccccccc"]
    assert strategy.name == "momentum"
    assert strategy.params["rsi_length"] == 14


@pytest.mark.asyncio
async def test_load_atr_breakout():
    """Test loading atr_breakout strategy."""
    rows = [
        {
            "id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "name": "atr_breakout",
            "type": "ta_lib",
            "params": '{"atr_length": 14, "ema_filter": 50}',
            "symbols": ["BTC", "ETH"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        }
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="paper")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "dddddddd-dddd-dddd-dddd-dddddddddddd" in engine.strategies
    strategy = engine.strategies["dddddddd-dddd-dddd-dddd-dddddddddddd"]
    assert strategy.name == "atr_breakout"
    assert strategy.params["atr_length"] == 14


@pytest.mark.asyncio
async def test_load_all_new_strategies():
    """Test loading all 4 new strategies together."""
    rows = [
        {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "volatility_expansion",
            "type": "ta_lib",
            "params": '{"keltner_len": 20}',
            "symbols": ["BTC"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
        {
            "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "name": "volume_range_breakout",
            "type": "ta_lib",
            "params": '{"lookback": 20}',
            "symbols": ["BTC"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
        {
            "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "name": "momentum",
            "type": "ta_lib",
            "params": '{"rsi_length": 14}',
            "symbols": ["BTC"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
        {
            "id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "name": "atr_breakout",
            "type": "ta_lib",
            "params": '{"atr_length": 14}',
            "symbols": ["BTC"],
            "timeframes": ["5m"],
            "version": "1.0.0",
            "mode": "paper",
            "is_live": True,
            "status": "active",
        },
    ]
    conn = FakeConn(rows)
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="paper")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert len(engine.strategies) == 4
    assert "volatility_expansion" in [s.name for s in engine.strategies.values()]
    assert "volume_range_breakout" in [s.name for s in engine.strategies.values()]
    assert "momentum" in [s.name for s in engine.strategies.values()]
    assert "atr_breakout" in [s.name for s in engine.strategies.values()]
