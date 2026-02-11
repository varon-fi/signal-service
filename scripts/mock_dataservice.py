"""Mock DataService for local E2E testing.

This module provides a minimal DataService implementation that emits
sample OHLC data for local end-to-end testing.
"""

import asyncio
import random
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from varon_fi.proto.varon_fi_pb2 import (
    OHLC,
    DataSubscription,
    TraceContext,
)
from varon_fi.proto.varon_fi_pb2_grpc import (
    DataServiceServicer,
    add_DataServiceServicer_to_server,
)


class MockDataService(DataServiceServicer):
    """Mock DataService that generates sample OHLC data."""

    def __init__(self):
        self.symbols = ['BTC', 'ETH']
        self.price_seeds = {'BTC': 50000.0, 'ETH': 3000.0}
        self._counter = 0

    async def StreamOHLC(self, request: DataSubscription, context):
        """Stream OHLC candles."""
        symbols = request.symbols if request.symbols else self.symbols
        timeframe = request.timeframe if request.timeframe else '5m'

        while True:
            self._counter += 1
            for symbol in symbols:
                # Generate sample OHLC with small random variation
                base_price = self.price_seeds[symbol]
                variation = random.uniform(-0.005, 0.005)
                close = base_price * (1 + variation)
                open_price = close * random.uniform(0.998, 1.002)
                high = max(open_price, close) * random.uniform(1.0, 1.003)
                low = min(open_price, close) * random.uniform(0.997, 1.0)

                now = datetime.now(timezone.utc)
                timestamp = Timestamp()
                timestamp.FromDatetime(now)

                trace = TraceContext(
                    correlation_id=f"mock-{self._counter}-{symbol}",
                    idempotency_key=f"mock-ohlc-{self._counter}-{symbol}",
                    source_service="mock-dataservice",
                    latency_ms=0,
                    timestamp=timestamp,
                )

                yield OHLC(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=random.uniform(10, 1000),
                    count=random.randint(1, 100),
                    trace=trace,
                )

            # Emit every 5 seconds
            await asyncio.sleep(5)

    async def StreamTrades(self, request: DataSubscription, context):
        """Stream raw trades."""
        while True:
            await asyncio.sleep(1)

    async def StreamOrderBook(self, request: DataSubscription, context):
        """Stream order book snapshots."""
        while True:
            await asyncio.sleep(1)


async def serve(port: int = 50051):
    """Start the mock DataService server."""
    server = grpc.aio.server()
    add_DataServiceServicer_to_server(MockDataService(), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    print(f"Mock DataService running on port {port}")
    print(f"Emitting OHLC for: {MockDataService().symbols}")
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    asyncio.run(serve(port))
