"""Local E2E test runner for Signal Service.

This script runs the Signal Service against mock DataService and ExecutionService
for end-to-end testing without external dependencies.

Prerequisites:
    pip install -e .
    # Ensure varon-fi package is installed with current proto

Usage:
    Terminal 1: python scripts/mock_dataservice.py 50051
    Terminal 2: python scripts/mock_executionservice.py 50053
    Terminal 3: python scripts/signal_service_e2e.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path for local imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

from signal_service.config.settings import Settings
from signal_service.grpc.server import SignalServiceServer
from signal_service.grpc.client import DataServiceClient
from signal_service.grpc.execution_client import ExecutionServiceClient
from signal_service.strategy.engine import StrategyEngine

load_dotenv()


async def main():
    """Run the Signal Service for local E2E testing."""
    settings = Settings(
        database_url=os.getenv("DATABASE_URL", "postgresql://postgres@localhost/varon_fi"),
        dataservice_addr=os.getenv("DATASERVICE_GRPC_ADDR", "localhost:50051"),
        signalservice_port=int(os.getenv("SIGNALSERVICE_GRPC_PORT", "50052")),
        executionservice_addr=os.getenv("EXECUTIONSERVICE_GRPC_ADDR", "localhost:50053"),
    )

    print("=" * 60)
    print("Signal Service - Local E2E Test")
    print("=" * 60)
    print(f"DataService:      {settings.dataservice_addr}")
    print(f"Signal Service:   port {settings.signalservice_port}")
    print(f"ExecutionService: {settings.executionservice_addr}")
    print("=" * 60)

    # Initialize strategy engine
    engine = StrategyEngine(settings.database_url)
    await engine.initialize()

    # Connect to ExecutionService for forwarding signals
    execution_client = ExecutionServiceClient(settings.executionservice_addr)
    await execution_client.connect()
    print(f"[OK] Connected to ExecutionService at {settings.executionservice_addr}")

    # Connect to DataService
    data_client = DataServiceClient(settings.dataservice_addr)
    await data_client.connect()
    print(f"[OK] Connected to DataService at {settings.dataservice_addr}")

    # Start SignalService gRPC server
    server = SignalServiceServer(engine, port=settings.signalservice_port)
    await server.start()
    print(f"[OK] Signal Service gRPC server running on port {settings.signalservice_port}")
    print()
    print("Waiting for OHLC data and generating signals...")
    print("(Press Ctrl+C to stop)")
    print()

    try:
        # Subscribe to OHLC stream and process
        async for ohlc in data_client.stream_ohlc():
            signal = await engine.process_candle(ohlc)
            if signal:
                print(f"[Signal Service] Generated signal: {signal.symbol} {signal.side}")
                print(f"                 Confidence: {signal.confidence:.2f}, Strategy: {signal.strategy_id}")
    except asyncio.CancelledError:
        print("\n[Shutdown] Received cancellation signal")
    except KeyboardInterrupt:
        print("\n[Shutdown] Interrupted by user")
    finally:
        print("[Shutdown] Cleaning up...")
        await server.stop()
        await data_client.disconnect()
        await execution_client.disconnect()
        await engine.shutdown()
        print("[Shutdown] Complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
