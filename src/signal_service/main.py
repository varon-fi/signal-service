#!/usr/bin/env python3
"""Signal Service main entry point.

Fixed to subscribe to all timeframes needed by active strategies.
"""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from signal_service.config.settings import Settings
from signal_service.grpc.server import SignalServiceServer
from signal_service.grpc.client import DataServiceClient
from signal_service.strategy.engine import StrategyEngine

load_dotenv()


async def stream_for_timeframe(
    engine: StrategyEngine,
    server: SignalServiceServer,
    dataservice_addr: str,
    symbols: list[str],
    timeframe: str,
):
    """Stream OHLC for a specific timeframe and process through engine."""
    client = DataServiceClient(dataservice_addr, symbols=symbols, timeframe=timeframe)
    await client.connect()
    try:
        async for ohlc in client.stream_ohlc():
            signal = await engine.process_candle(ohlc)
            if signal:
                await server.emit_signal(signal)
    except asyncio.CancelledError:
        raise
    finally:
        await client.disconnect()


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

    # Get required subscriptions from strategies
    subscriptions = engine.get_required_subscriptions()
    if not subscriptions:
        raise RuntimeError("No active strategies found - nothing to subscribe to")

    print(f"📊 Signal Service starting with subscriptions:")
    for timeframe, symbols in subscriptions.items():
        print(f"   - {timeframe}: {symbols}")

    # Start SignalService gRPC server
    server = SignalServiceServer(engine, port=settings.signalservice_port)
    await server.start()

    # Create streaming tasks for each timeframe
    tasks = []
    for timeframe, symbols in subscriptions.items():
        task = asyncio.create_task(
            stream_for_timeframe(
                engine, server, settings.dataservice_addr, symbols, timeframe
            ),
            name=f"stream_{timeframe}",
        )
        tasks.append(task)

    try:
        # Run all streaming tasks concurrently
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        # Cancel all streaming tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.stop()
        await engine.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
