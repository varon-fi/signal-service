"""Tests for gRPC signal stream emission behavior."""

import asyncio

import pytest

from varon_fi import Signal
from varon_fi.proto.varon_fi_pb2 import SignalSubscription
from signal_service.grpc.server import SignalServiceServer
from signal_service.strategy.engine import StrategyEngine


def _make_signal(idempotency_key: str) -> Signal:
    signal = Signal(side="long", price=50000.0, confidence=0.8, idempotency_key=idempotency_key)
    signal.strategy_id = "s1"
    signal.strategy_version = "1.0.0"
    signal.symbol = "BTC"
    signal.timeframe = "5m"
    return signal


@pytest.mark.asyncio
async def test_emit_signal_uses_persisted_signal_id_when_available():
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    server = SignalServiceServer(engine)
    queue: asyncio.Queue = asyncio.Queue()
    server._subscribers.append((queue, SignalSubscription()))

    signal = _make_signal("idem-1")
    signal.signal_db_id = "db-signal-123"

    await server.emit_signal(signal)

    emitted = await asyncio.wait_for(queue.get(), timeout=0.2)
    assert emitted.signal_id == "db-signal-123"


@pytest.mark.asyncio
async def test_emit_signal_falls_back_when_persisted_signal_id_unavailable():
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    server = SignalServiceServer(engine)
    queue: asyncio.Queue = asyncio.Queue()
    server._subscribers.append((queue, SignalSubscription()))

    signal = _make_signal("idem-2")

    await server.emit_signal(signal)

    emitted = await asyncio.wait_for(queue.get(), timeout=0.2)
    assert emitted.signal_id == "idem-2"
