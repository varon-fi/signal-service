"""Signal Service main entry point."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from signal_service.config.settings import Settings
from signal_service.grpc.server import SignalServiceServer
from signal_service.grpc.client import DataServiceClient
from signal_service.strategy.engine import StrategyEngine

load_dotenv()


async def main():
    """Run the Signal Service."""
    settings = Settings(
        database_url=os.getenv("DATABASE_URL", "postgresql://postgres@localhost/varon_fi"),
        dataservice_addr=os.getenv("DATASERVICE_GRPC_ADDR", "localhost:50051"),
        signalservice_port=int(os.getenv("SIGNALSERVICE_GRPC_PORT", "50052")),
        trading_mode=os.getenv("SIGNALSERVICE_TRADING_MODE", "live"),
    )
    
    # Initialize strategy engine (loads strategies from DB)
    engine = StrategyEngine(settings.database_url, mode=settings.trading_mode)
    await engine.initialize()
    
    # Connect to DataService
    data_client = DataServiceClient(settings.dataservice_addr)
    await data_client.connect()
    
    # Start SignalService gRPC server
    server = SignalServiceServer(engine, port=settings.signalservice_port)
    await server.start()
    
    try:
        # Subscribe to OHLC stream and process
        async for ohlc in data_client.stream_ohlc():
            signal = await engine.process_candle(ohlc)
            if signal:
                await server.emit_signal(signal)
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
        await data_client.disconnect()
        await engine.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
