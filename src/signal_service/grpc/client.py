"""gRPC client for DataService."""

from typing import AsyncIterator

import grpc
from structlog import get_logger

from varon_fi.proto.varon_fi_pb2 import DataSubscription
from varon_fi.proto.varon_fi_pb2_grpc import DataServiceStub

logger = get_logger(__name__)


class DataServiceClient:
    """Client for consuming DataService gRPC streams."""
    
    def __init__(self, addr: str, symbols: list = None, timeframe: str = "5m"):
        self.addr = addr
        self.symbols = symbols or ["BTC", "ETH"]
        self.timeframe = timeframe
        self.channel = None
        self.stub = None
        
    async def connect(self):
        """Connect to DataService."""
        self.channel = grpc.aio.insecure_channel(self.addr)
        self.stub = DataServiceStub(self.channel)
        logger.info("Connected to DataService", addr=self.addr)
        
    async def stream_ohlc(self) -> AsyncIterator[dict]:
        """Stream OHLC candles from DataService."""
        request = DataSubscription(
            symbols=self.symbols,
            timeframe=self.timeframe,
            include_trades=False,
            include_orderbook=False,
        )
        
        async for ohlc in self.stub.StreamOHLC(request):
            yield {
                'symbol': ohlc.symbol,
                'timeframe': ohlc.timeframe,
                'timestamp': ohlc.timestamp,
                'open': ohlc.open,
                'high': ohlc.high,
                'low': ohlc.low,
                'close': ohlc.close,
                'volume': ohlc.volume,
                'count': ohlc.count,
            }
        
    async def disconnect(self):
        """Close connection."""
        if self.channel:
            await self.channel.close()
        logger.info("Disconnected from DataService")
