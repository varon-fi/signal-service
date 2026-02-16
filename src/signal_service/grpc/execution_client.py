"""gRPC client for ExecutionService."""

from typing import Optional
import uuid
import sys

import grpc
from structlog import get_logger
from google.protobuf.timestamp_pb2 import Timestamp

# Import from varon-fi package
from varon_fi.proto import varon_fi_pb2 as _varon_fi_pb2
sys.modules.setdefault("varon_fi_pb2", _varon_fi_pb2)

from varon_fi.proto.varon_fi_pb2 import (
    TradeSignal,
    OrderRequest,
    OrderStatus,
    TradingMode,
    TraceContext,
)
from varon_fi.proto.varon_fi_pb2_grpc import ExecutionServiceStub

logger = get_logger(__name__)


class ExecutionServiceClient:
    """Client for sending signals to ExecutionService (Orders Service).

    Handles connection management, retry logic, timeout handling,
    and correlation ID chaining for observability.
    """

    def __init__(
        self,
        addr: str,
        timeout: float = 5.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        self.addr = addr
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[ExecutionServiceStub] = None

    async def connect(self):
        """Connect to ExecutionService."""
        self.channel = grpc.aio.insecure_channel(self.addr)
        self.stub = ExecutionServiceStub(self.channel)
        logger.info("Connected to ExecutionService", addr=self.addr)

    def _convert_side(self, signal_side: str) -> str:
        """Convert TradeSignal side ('buy'/'sell') to OrderRequest side ('long'/'short')."""
        mapping = {
            "buy": "long",
            "sell": "short",
        }
        return mapping.get(signal_side.lower(), signal_side.lower())

    def _convert_mode(self, mode_value) -> TradingMode:
        """Convert mode string or enum to TradingMode enum."""
        if isinstance(mode_value, TradingMode):
            return mode_value
        mode_str = str(mode_value).lower()
        if mode_str == "live":
            return TradingMode.LIVE
        return TradingMode.PAPER

    def _to_order_request(self, signal: TradeSignal) -> OrderRequest:
        """Convert TradeSignal protobuf to OrderRequest protobuf.

        Args:
            signal: The TradeSignal from strategy engine

        Returns:
            OrderRequest for ExecutionService.ExecuteSignal
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        # Build TraceContext
        trace = TraceContext(
            correlation_id=signal.trace.correlation_id if signal.trace.correlation_id else str(uuid.uuid4()),
            idempotency_key=signal.idempotency_key if signal.idempotency_key else str(uuid.uuid4()),
            source_service="signal-service",
            latency_ms=0,  # TODO: Track actual latency
            timestamp=timestamp,
        )

        # Convert TradeSignal to OrderRequest
        # Note: TradeSignal doesn't have size/order_type, so we use defaults
        return OrderRequest(
            signal_id=signal.signal_id,
            strategy_id=signal.strategy_id,
            strategy_version=signal.strategy_version,
            symbol=signal.symbol,
            side=self._convert_side(signal.side),
            size=0.0,  # TODO: Size should come from config or signal
            price=signal.price if signal.price else 0.0,  # 0 for market order
            order_type="market" if signal.price == 0 or signal.price is None else "limit",
            mode=self._convert_mode(signal.mode),
            risk_checks={},  # TODO: Populate from risk module
            trace=trace,
        )

    async def execute_signal(
        self,
        signal: TradeSignal,
    ) -> OrderStatus:
        """Send a signal to ExecutionService, converting to OrderRequest.

        Args:
            signal: The TradeSignal to execute

        Returns:
            OrderStatus response from ExecutionService

        Raises:
            grpc.RpcError: If all retries exhausted
        """
        order_request = self._to_order_request(signal)
        correlation_id = order_request.trace.correlation_id

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(
                    "Sending signal to ExecutionService",
                    attempt=attempt,
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    strategy_id=signal.strategy_id,
                    symbol=signal.symbol,
                    side=signal.side,
                    mode=order_request.mode,
                )

                response = await self.stub.ExecuteSignal(
                    order_request,
                    timeout=self.timeout,
                )

                logger.info(
                    "Signal accepted by ExecutionService",
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    order_id=response.order_id if response.order_id else None,
                    status=response.status,
                )
                return response

            except grpc.RpcError as e:
                last_error = e
                logger.warning(
                    "ExecutionService call failed",
                    attempt=attempt,
                    max_retries=self.max_retries,
                    signal_id=signal.signal_id,
                    correlation_id=correlation_id,
                    error_code=e.code(),
                    error_details=e.details(),
                )

                if attempt < self.max_retries:
                    import asyncio
                    await asyncio.sleep(self.retry_delay * attempt)  # Exponential backoff

        # All retries exhausted
        logger.error(
            "ExecutionService call failed after all retries",
            signal_id=signal.signal_id,
            correlation_id=correlation_id,
            max_retries=self.max_retries,
            error_code=last_error.code() if last_error else None,
            error_details=last_error.details() if last_error else None,
        )
        raise last_error

    async def disconnect(self):
        """Close connection to ExecutionService."""
        if self.channel:
            await self.channel.close()
            self.channel = None
            self.stub = None
        logger.info("Disconnected from ExecutionService")
