"""Updated Signal Service main for local E2E testing."""

import asyncio
import os

from dotenv import load_dotenv

from signal_service.config.settings import Settings
from signal_service.grpc.server import SignalServiceServer
from signal_service.grpc.client import DataServiceClient
from signal_service.strategy.engine import StrategyEngine

load_dotenv()


async def main():
    """Run the Signal Service for local E2E testing."""
    settings = Settings(
        database_url=os.getenv("DATABASE_URL", "postgresql:///varon_fi?user=varon"),
        dataservice_addr=os.getenv("DATASERVICE_GRPC_ADDR", "localhost:50051"),
        signalservice_port=int(os.getenv("SIGNALSERVICE_GRPC_PORT", "50052")),
    )
    
    # Initialize strategy engine
    engine = StrategyEngine(settings.database_url)
    await engine.initialize()
    
    # Connect to DataService
    data_client = DataServiceClient(settings.dataservice_addr)
    await data_client.connect()
    
    # Start SignalService gRPC server
    server = SignalServiceServer(engine, port=settings.signalservice_port)
    await server.start()
    
    print(f"Signal Service running on port {settings.signalservice_port}")
    print(f"Connected to DataService at {settings.dataservice_addr}")
    
    try:
        # Subscribe to OHLC stream and process
        async for ohlc in data_client.stream_ohlc():
            signals = await engine.process_candle_signals(ohlc)
            for signal in signals:
                await server.emit_signal(signal)
                print(f"[Signal Service] Generated signal: {signal.symbol} {signal.side}")
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
        await data_client.disconnect()
        await engine.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
