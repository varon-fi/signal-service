"""gRPC client for DataService."""

from typing import AsyncIterator

import grpc
from structlog import get_logger

logger = get_logger(__name__)


class DataServiceClient:
    """Client for consuming DataService gRPC streams."""
    
    def __init__(self, addr: str):
        self.addr = addr
        self.channel = None
        self.stub = None
        
    async def connect(self):
        """Connect to DataService."""
        self.channel = grpc.aio.insecure_channel(self.addr)
        # stub import will be generated from proto
        logger.info("Connected to DataService", addr=self.addr)
        
    async def stream_ohlc(self) -> AsyncIterator[dict]:
        """Stream OHLC candles from DataService."""
        # TODO: Implement when proto is ready
        # from generated import DataServiceStub, OHLCSubscription
        # request = OHLCSubscription(symbols=['BTC', 'ETH'], timeframe='5m')
        # async for ohlc in self.stub.StreamOHLC(request):
        #     yield {
        #         'symbol': ohlc.symbol,
        #         'timeframe': ohlc.timeframe,
        #         'timestamp': ohlc.timestamp,
        #         'open': ohlc.open,
        #         'high': ohlc.high,
        #         'low': ohlc.low,
        #         'close': ohlc.close,
        #         'volume': ohlc.volume,
        #     }
        pass
        
    async def disconnect(self):
        """Close connection."""
        if self.channel:
            await self.channel.close()
        logger.info("Disconnected from DataService")
