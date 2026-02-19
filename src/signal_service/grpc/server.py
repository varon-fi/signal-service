"""gRPC server for SignalService."""

import asyncio
from typing import Optional

import grpc
from structlog import get_logger

from varon_fi import Signal
from varon_fi.proto.varon_fi_pb2 import TradeSignal, SignalAck, SignalSubscription
from varon_fi.proto.varon_fi_pb2_grpc import (
    SignalServiceServicer,
    add_SignalServiceServicer_to_server,
)
from signal_service.strategy.engine import StrategyEngine

logger = get_logger(__name__)


class SignalServiceImpl(SignalServiceServicer):
    """gRPC servicer for SignalService."""

    def __init__(self, server: "SignalServiceServer"):
        self.server = server

    async def PublishSignal(self, request: TradeSignal, context) -> SignalAck:
        """Publish a signal (used by Signal Service to emit)."""
        await self.server._broadcast(request)
        return SignalAck(success=True, message="published", signal_id=request.signal_id)

    async def StreamSignals(self, request: SignalSubscription, context):
        """Subscribe to signal stream (used by Orders Service)."""
        queue: asyncio.Queue = asyncio.Queue()
        sub = (queue, request)
        self.server._subscribers.append(sub)
        logger.info("New signal subscriber", strategies=request.strategy_ids, symbols=request.symbols)

        try:
            while True:
                try:
                    signal: TradeSignal = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Check if context is still valid (client disconnected if cancelled)
                    if hasattr(context, 'cancelled') and context.cancelled():
                        break
                    if hasattr(context, 'is_active') and not context.is_active():
                        break
                    continue

                # Filter by strategy/symbol if provided
                if request.strategy_ids and signal.strategy_id not in request.strategy_ids:
                    continue
                if request.symbols and signal.symbol not in request.symbols:
                    continue

                yield signal
        except asyncio.CancelledError:
            logger.info("Signal stream cancelled by client")
        finally:
            if sub in self.server._subscribers:
                self.server._subscribers.remove(sub)
            logger.info("Signal subscriber disconnected")


class SignalServiceServer:
    """gRPC server exposing SignalService."""

    def __init__(self, engine: StrategyEngine, port: int = 50052):
        self.engine = engine
        self.port = port
        self.server = None
        self._subscribers: list[tuple[asyncio.Queue, SignalSubscription]] = []
        self._impl = SignalServiceImpl(self)

    async def start(self):
        """Start the gRPC server."""
        self.server = grpc.aio.server()
        add_SignalServiceServicer_to_server(self._impl, self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        await self.server.start()
        logger.info("SignalService started", port=self.port)

    async def stop(self):
        """Stop the gRPC server."""
        if self.server:
            await self.server.stop(grace=5)
        logger.info("SignalService stopped")

    async def _broadcast(self, trade_signal: TradeSignal):
        """Broadcast signal to all subscribers."""
        for queue, _ in list(self._subscribers):
            await queue.put(trade_signal)

    async def emit_signal(self, signal: Signal):
        """Emit signal to all connected subscribers."""
        trade_signal = self.engine._to_trade_signal(signal)
        await self._broadcast(trade_signal)
        logger.info(
            "Signal emitted",
            symbol=signal.symbol,
            side=signal.side,
            subscribers=len(self._subscribers),
        )
