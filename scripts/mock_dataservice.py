"""Mock DataService for local E2E testing.

This module provides a minimal DataService implementation that emits
sample OHLC data for local end-to-end testing.
"""

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from varon_fi.proto.varon_fi_pb2 import (
    MarketData,
    DataSubscription,
    Trade,
    OrderBookSnapshot,
    OrderBookLevel,
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
        
    async def StreamMarketData(self, request: DataSubscription, context):
        """Stream OHLC candles."""
        symbols = request.symbols if request.symbols else self.symbols
        
        while True:
            for symbol in symbols:
                # Generate sample OHLC with small random variation
                base_price = self.price_seeds[symbol]
                variation = (hash(datetime.now().isoformat()) % 100 - 50) / 1000
                price = base_price * (1 + variation)
                
                now = datetime.now(timezone.utc)
                timestamp = Timestamp()
                timestamp.FromDatetime(now)
                
                yield MarketData(
                    timestamp=timestamp,
                    symbol=symbol,
                    open=price * 0.998,
                    high=price * 1.002,
                    low=price * 0.997,
                    close=price,
                    volume=100.0,
                    source="mock",
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
    
    async def GetHistoricalData(self, request, context):
        """Get historical data."""
        from varon_fi.proto.varon_fi_pb2 import HistoricalDataResponse
        return HistoricalDataResponse()


async def serve(port: int = 50051):
    """Start the mock DataService server."""
    server = grpc.aio.server()
    add_DataServiceServicer_to_server(MockDataService(), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    print(f"Mock DataService running on port {port}")
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    asyncio.run(serve(port))
