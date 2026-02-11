"""Mock ExecutionService for local E2E testing.

This module provides a minimal ExecutionService implementation that
accepts OrderRequest messages (from ExecuteSignal RPC) and logs them for verification.

Aligned with current proto:
- ExecuteSignal takes OrderRequest (not TradeSignal)
- Uses TradingMode enum (PAPER=0, LIVE=1)
- Returns OrderStatus
"""

import asyncio
import uuid
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from varon_fi.proto.varon_fi_pb2 import (
    OrderRequest,
    OrderStatus,
    OrderSubscription,
    Position,
    OrderStatusRequest,
    CancelOrderRequest,
    CancelOrderResponse,
    TradingMode,
    TraceContext,
)
from varon_fi.proto.varon_fi_pb2_grpc import (
    ExecutionServiceServicer,
    add_ExecutionServiceServicer_to_server,
)


class MockExecutionService(ExecutionServiceServicer):
    """Mock ExecutionService that logs received OrderRequests."""

    def __init__(self):
        self.received_orders = []
        self.order_id_counter = 0

    async def ExecuteSignal(self, request: OrderRequest, context):
        """Execute an OrderRequest (aligned with current proto)."""
        self.order_id_counter += 1
        order_id = f"mock-order-{self.order_id_counter}"

        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        trace = TraceContext(
            correlation_id=request.trace.correlation_id if request.trace.correlation_id else str(uuid.uuid4()),
            idempotency_key=request.trace.idempotency_key if request.trace.idempotency_key else str(uuid.uuid4()),
            source_service="mock-executionservice",
            latency_ms=0,
            timestamp=timestamp,
        )

        # Determine mode string for logging
        mode_str = "LIVE" if request.mode == TradingMode.LIVE else "PAPER"

        self.received_orders.append({
            'order_id': order_id,
            'signal_id': request.signal_id,
            'strategy_id': request.strategy_id,
            'strategy_version': request.strategy_version,
            'symbol': request.symbol,
            'side': request.side,
            'size': request.size,
            'price': request.price,
            'order_type': request.order_type,
            'mode': mode_str,
            'correlation_id': trace.correlation_id,
            'timestamp': now.isoformat(),
        })

        print(f"[ExecutionService] Received OrderRequest:")
        print(f"  Signal: {request.signal_id}")
        print(f"  Strategy: {request.strategy_id} (v{request.strategy_version})")
        print(f"  Symbol: {request.symbol} {request.side} @ {request.price}")
        print(f"  Size: {request.size}, Type: {request.order_type}, Mode: {mode_str}")
        print(f"  Total orders received: {len(self.received_orders)}")

        return OrderStatus(
            order_id=order_id,
            signal_id=request.signal_id,
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            side=request.side,
            size=request.size,
            price=request.price,
            status="filled",
            filled_size=request.size,
            filled_price=request.price,
            fee_paid=0.0,
            mode=request.mode,
            trace=trace,
        )

    async def StreamOrders(self, request: OrderSubscription, context):
        """Stream order updates."""
        while True:
            await asyncio.sleep(1)

    async def GetOrderStatus(self, request: OrderStatusRequest, context):
        """Get order by ID."""
        return OrderStatus(
            order_id=request.order_id,
            signal_id="",
            strategy_id="",
            symbol="",
            side="",
            size=0.0,
            price=0.0,
            status="filled",
            filled_size=0.0,
            filled_price=0.0,
            fee_paid=0.0,
            mode=TradingMode.PAPER,
        )

    async def CancelOrder(self, request: CancelOrderRequest, context):
        """Cancel an order."""
        return CancelOrderResponse(
            success=True,
            message="Order cancelled",
        )

    async def StreamPositions(self, request, context):
        """Stream position updates."""
        while True:
            await asyncio.sleep(1)


async def serve(port: int = 50053):
    """Start the mock ExecutionService server."""
    server = grpc.aio.server()
    add_ExecutionServiceServicer_to_server(MockExecutionService(), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    print(f"Mock ExecutionService running on port {port}")
    print("Waiting for OrderRequests from Signal Service...")
    print("(ExecuteSignal RPC expects OrderRequest per current proto)")
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50053
    asyncio.run(serve(port))
