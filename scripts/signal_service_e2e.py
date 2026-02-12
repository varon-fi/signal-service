"""Local E2E test runner for Signal Service.

This script runs the Signal Service against mock DataService and ExecutionService
for end-to-end testing without external dependencies.

Prerequisites:
    pip install -e .
    # Ensure varon-fi package is installed with current proto

Usage (mock mode - no DB required):
    Terminal 1: python scripts/mock_dataservice.py 50051
    Terminal 2: python scripts/mock_executionservice.py 50053
    Terminal 3: python scripts/signal_service_e2e.py --mock

Usage (with database):
    # Ensure Postgres is running with seeded strategies
    Terminal 1: python scripts/mock_dataservice.py 50051
    Terminal 2: python scripts/mock_executionservice.py 50053
    Terminal 3: python scripts/signal_service_e2e.py
"""

import argparse
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

load_dotenv()


async def run_with_mock_engine(settings: Settings):
    """Run with mock engine (no DB required)."""
    from signal_service.strategy.mock_engine import MockStrategyEngine
    
    print("[Mode] Using MockStrategyEngine (no database required)")
    
    # Initialize mock strategy engine
    engine = MockStrategyEngine()
    await engine.initialize()

    # Connect to ExecutionService for forwarding signals
    execution_client = ExecutionServiceClient(settings.executionservice_addr)
    await execution_client.connect()
    engine.execution_client = execution_client
    print(f"[OK] Connected to ExecutionService at {settings.executionservice_addr}")

    # Connect to DataService
    data_client = DataServiceClient(
        settings.dataservice_addr,
        symbols=["BTC", "ETH"],
        timeframe="5m",
    )
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


async def run_with_db_engine(settings: Settings):
    """Run with database-backed engine."""
    from signal_service.strategy.engine import StrategyEngine
    
    print("[Mode] Using StrategyEngine with database")
    print(f"       Database: {settings.database_url}")
    
    # Initialize strategy engine
    engine = StrategyEngine(settings.database_url)
    await engine.initialize()

    # Connect to ExecutionService for forwarding signals
    execution_client = ExecutionServiceClient(settings.executionservice_addr)
    await execution_client.connect()
    print(f"[OK] Connected to ExecutionService at {settings.executionservice_addr}")

    # Connect to DataService
    data_client = DataServiceClient(
        settings.dataservice_addr,
        symbols=["BTC", "ETH"],
        timeframe="5m",
    )
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


async def main():
    """Run the Signal Service for local E2E testing."""
    parser = argparse.ArgumentParser(description="Signal Service E2E Test Runner")
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use mock engine (no database required)",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql://postgres@localhost/varon_fi"),
        help="Database URL (used when not in mock mode)",
    )
    parser.add_argument(
        "--dataservice-addr",
        default=os.getenv("DATASERVICE_GRPC_ADDR", "localhost:50051"),
        help="DataService gRPC address",
    )
    parser.add_argument(
        "--executionservice-addr",
        default=os.getenv("EXECUTIONSERVICE_GRPC_ADDR", "localhost:50053"),
        help="ExecutionService gRPC address",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SIGNALSERVICE_GRPC_PORT", "50052")),
        help="Signal Service gRPC port",
    )
    args = parser.parse_args()

    settings = Settings(
        database_url=args.database_url,
        dataservice_addr=args.dataservice_addr,
        signalservice_port=args.port,
        executionservice_addr=args.executionservice_addr,
    )

    print("=" * 60)
    print("Signal Service - Local E2E Test")
    print("=" * 60)
    print(f"DataService:      {settings.dataservice_addr}")
    print(f"Signal Service:   port {settings.signalservice_port}")
    print(f"ExecutionService: {settings.executionservice_addr}")
    print("=" * 60)

    if args.mock:
        await run_with_mock_engine(settings)
    else:
        await run_with_db_engine(settings)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
