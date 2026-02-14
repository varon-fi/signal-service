"""Tests for StrategyEngine strategy loading."""

import pytest

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
    engine = StrategyEngine("postgresql://localhost/varon_fi", mode="live")
    engine.pool = FakePool(conn)

    await engine._load_strategies()

    assert "11111111-1111-1111-1111-111111111111" in engine.strategies
    assert conn.last_args == ("live",)
    assert "mode = $1" in conn.last_query
