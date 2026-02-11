"""Mock ExecutionService for local E2E testing.

This module provides a minimal ExecutionService implementation that
accepts TradeSignals and logs them for verification.
"""

import asyncio
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from varon_fi.proto.varon_fi_pb2 import (
    TradeSignal,
    OrderAck,
    OrderRequest,
    OrderStatus,
    Position,
    GetOrderRequest,
    CancelOrderRequest,
    CancelOrderResponse,
)
from varon_fi.proto.varon_fi_pb2_grpc import (
    ExecutionServiceServicer,
    add_ExecutionServiceServicer_to_server,
)


class MockExecutionService(ExecutionServiceServicer):
    """Mock ExecutionService that logs received signals."""
    
    def __init__(self):
        self.received_signals = []
        self.order_id_counter = 0
        
    async def ExecuteSignal(self, request: TradeSignal, context):
        """Execute a TradeSignal."""
        self.order_id_counter += 1
        order_id = f"order-{self.order_id_counter}"
        
        self.received_signals.append({
            'order_id': order_id,
            'signal_id': request.signal_id,
            'strategy_id': request.strategy_id,
            'symbol': request.symbol,
            'side': request.side,
            'price': request.price,
            'timestamp': datetime.now().isoformat(),
        })
        
        print(f"[ExecutionService] Received signal: {request.symbol} {request.side} @ {request.price}")
        print(f"[ExecutionService] Total signals received: {len(self.received_signals)}")
        
        return OrderAck(
            success=True,
            message="Signal received",
            order_id=order_id,
            correlation_id=request.correlation_id,
        )
    
    async def PlaceOrder(self, request: OrderRequest, context):
        """Place a direct order."""
        self.order_id_counter += 1
        return OrderAck(
            success=True,
            message="Order placed",
            order_id=f"order-{self.order_id_counter}",
            correlation_id=request.correlation_id,
        )
    
    async def StreamOrders(self, request, context):
        """Stream order updates."""
        while True:
            await asyncio.sleep(1)
    
    async def GetOrder(self, request: GetOrderRequest, context):
        """Get order by ID."""
        return OrderStatus(
            order_id=request.order_id,
            status="filled",
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
    print("Waiting for TradeSignals from Signal Service...")
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50053
    asyncio.run(serve(port))
