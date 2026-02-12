"""Mock ExecutionService for local E2E testing.

This module provides a minimal ExecutionService implementation that
accepts OrderRequests and logs them for verification.
"""

import asyncio
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from varon_fi.proto.varon_fi_pb2 import (
    OrderRequest,
    OrderStatus,
    Position,
    OrderStatusRequest,
    CancelOrderRequest,
    CancelOrderResponse,
    OrderSubscription,
    TradingMode,
    TraceContext,
)
from varon_fi.proto.varon_fi_pb2_grpc import (
    ExecutionServiceServicer,
    add_ExecutionServiceServicer_to_server,
)


class MockExecutionService(ExecutionServiceServicer):
    """Mock ExecutionService that logs received orders."""
    
    def __init__(self):
        self.received_orders = []
        self.order_id_counter = 0
        
    async def ExecuteSignal(self, request: OrderRequest, context):
        """Execute an OrderRequest (creates order)."""
        self.order_id_counter += 1
        order_id = f"order-{self.order_id_counter}"
        
        self.received_orders.append({
            'order_id': order_id,
            'signal_id': request.signal_id,
            'strategy_id': request.strategy_id,
            'symbol': request.symbol,
            'side': request.side,
            'size': request.size,
            'price': request.price,
            'order_type': request.order_type,
            'mode': 'paper' if request.mode == TradingMode.PAPER else 'live',
            'timestamp': datetime.now().isoformat(),
        })
        
        print(f"[ExecutionService] Received order: {request.symbol} {request.side} {request.size} @ {request.price}")
        print(f"[ExecutionService] Total orders received: {len(self.received_orders)}")
        
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)
        
        trace = TraceContext(
            timestamp=timestamp,
            source_service="mock-executionservice",
            correlation_id=request.trace.correlation_id if request.trace else "",
            idempotency_key=request.trace.idempotency_key if request.trace else "",
        )
        
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
    
    async def GetOrderStatus(self, request: OrderStatusRequest, context):
        """Get order status."""
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)
        
        return OrderStatus(
            order_id=request.order_id,
            status="filled",
            trace=TraceContext(timestamp=timestamp),
        )
    
    async def CancelOrder(self, request: CancelOrderRequest, context):
        """Cancel an order."""
        return CancelOrderResponse(
            success=True,
            message="Order cancelled",
        )
    
    async def StreamOrders(self, request: OrderSubscription, context):
        """Stream order updates."""
        while True:
            await asyncio.sleep(1)
    
    async def StreamPositions(self, request: OrderSubscription, context):
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
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50053
    asyncio.run(serve(port))
