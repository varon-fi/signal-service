"""gRPC server for SignalService."""

import asyncio
from typing import Optional

import grpc
from structlog import get_logger

from varon_fi import Signal
from signal_service.strategy.engine import StrategyEngine

logger = get_logger(__name__)


class SignalServiceServer:
    """gRPC server exposing SignalService."""
    
    def __init__(self, engine: StrategyEngine, port: int = 50052):
        self.engine = engine
        self.port = port
        self.server = None
        self._subscribers: list = []
        
    async def start(self):
        """Start the gRPC server."""
        self.server = grpc.aio.server()
        # TODO: Add service implementation when proto is ready
        # from generated import SignalServiceServicer, add_SignalServiceServicer_to_server
        # add_SignalServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        await self.server.start()
        logger.info("SignalService started", port=self.port)
        
    async def stop(self):
        """Stop the gRPC server."""
        if self.server:
            await self.server.stop(grace_period=5)
        logger.info("SignalService stopped")
        
    async def emit_signal(self, signal: Signal):
        """Emit signal to all connected subscribers."""
        # TODO: Implement when proto is ready
        # trade_signal = TradeSignal(
        #     strategy_id=signal.strategy_id,
        #     strategy_version=signal.strategy_version,
        #     symbol=signal.symbol,
        #     timeframe=signal.timeframe,
        #     side=signal.side,
        #     price=signal.price,
        #     confidence=signal.confidence,
        #     correlation_id=signal.correlation_id,
        #     idempotency_key=signal.idempotency_key,
        #     meta=json.dumps(signal.meta),
        # )
        logger.info("Signal emitted", 
                   symbol=signal.symbol, 
                   side=signal.side,
                   subscribers=len(self._subscribers))
