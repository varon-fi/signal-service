"""Strategy engine - loads and executes strategies."""

import json
import uuid
from typing import Optional

import asyncpg
import pandas as pd
from structlog import get_logger
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone

from signal_service.strategy.base import BaseStrategy, Signal
from signal_service.grpc.execution_client import ExecutionServiceClient
from varon_fi.proto.varon_fi_pb2 import TradeSignal, TradingMode, TraceContext

logger = get_logger(__name__)


class StrategyEngine:
    """Manages live strategies and generates signals."""
    
    def __init__(
        self, 
        database_url: str,
        execution_client: Optional[ExecutionServiceClient] = None,
    ):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
        self.strategies: dict[str, BaseStrategy] = {}
        self.execution_client = execution_client
        
    async def connect_execution_service(self, addr: str):
        """Connect to ExecutionService for signal forwarding."""
        self.execution_client = ExecutionServiceClient(addr)
        await self.execution_client.connect()
        logger.info("ExecutionService client connected", addr=addr)
        
    async def initialize(self):
        """Initialize DB connection and load active strategies."""
        self.pool = await asyncpg.create_pool(self.database_url)
        await self._load_strategies()
        logger.info("StrategyEngine initialized", strategy_count=len(self.strategies))
        
    async def _load_strategies(self):
        """Load active live strategies from database."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, name, type, params, symbols, timeframes, strategy_version
                FROM strategies
                WHERE is_live = true AND status = 'active'
            """)
            
        for row in rows:
            strategy = self._create_strategy(row)
            if strategy:
                self.strategies[str(row['id'])] = strategy
                
    def _create_strategy(self, row: asyncpg.Record) -> Optional[BaseStrategy]:
        """Instantiate a strategy from DB row."""
        from signal_service.strategy.mtf_confluence import MtfConfluenceStrategy
        
        strategy_map = {
            'mtf_confluence': MtfConfluenceStrategy,
            # Add more strategy types here
        }
        
        strategy_class = strategy_map.get(row['name'])
        if not strategy_class:
            logger.warning("Unknown strategy type", name=row['name'])
            return None
            
        params = json.loads(row['params']) if isinstance(row['params'], str) else row['params']
        return strategy_class(
            strategy_id=str(row['id']),
            name=row['name'],
            version=row['strategy_version'],
            symbols=row['symbols'],
            timeframes=row['timeframes'],
            params=params,
        )
        
    async def process_candle(self, ohlc: dict) -> Optional[Signal]:
        """Process an OHLC candle and return signal if generated."""
        symbol = ohlc.get('symbol')
        timeframe = ohlc.get('timeframe')
        
        for strategy_id, strategy in self.strategies.items():
            if symbol not in strategy.symbols or timeframe not in strategy.timeframes:
                continue
                
            # Fetch recent history for this symbol/timeframe
            history = await self._fetch_history(symbol, timeframe, bars=200)
            
            signal = strategy.on_candle(ohlc, history)
            if signal:
                signal.strategy_id = strategy_id
                signal.strategy_version = strategy.version
                signal.symbol = symbol
                signal.timeframe = timeframe
                
                # Persist to database
                await self._persist_signal(signal)
                
                # Send to ExecutionService if connected
                if self.execution_client:
                    try:
                        trade_signal = self._to_trade_signal(signal)
                        await self.execution_client.execute_signal(trade_signal)
                    except Exception as e:
                        # Log error but don't fail - signal is already persisted
                        logger.error(
                            "Failed to send signal to ExecutionService",
                            signal_id=signal.idempotency_key,
                            correlation_id=signal.correlation_id,
                            error=str(e),
                        )
                
                logger.info("Signal generated", 
                          strategy=strategy.name, 
                          symbol=symbol, 
                          side=signal.side,
                          correlation_id=signal.correlation_id)
                return signal
                
        return None
        
    def _to_trade_signal(self, signal: Signal) -> TradeSignal:
        """Convert internal Signal to TradeSignal protobuf matching PR#7 proto."""
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        # Convert mode string to TradingMode enum
        mode_str = (signal.meta.get("mode", "live") if signal.meta else "live").lower()
        mode = TradingMode.LIVE if mode_str == "live" else TradingMode.PAPER

        # Build TraceContext with correlation and idempotency keys
        trace = TraceContext(
            correlation_id=signal.correlation_id if signal.correlation_id else str(uuid.uuid4()),
            idempotency_key=signal.idempotency_key if signal.idempotency_key else str(uuid.uuid4()),
            source_service="signal-service",
            latency_ms=0,  # TODO: Track actual latency
            timestamp=timestamp,
        )

        return TradeSignal(
            signal_id=signal.idempotency_key if signal.idempotency_key else str(uuid.uuid4()),
            strategy_id=signal.strategy_id or "",
            strategy_version=signal.strategy_version or "",
            symbol=signal.symbol or "",
            timeframe=signal.timeframe or "5m",
            side=signal.side,
            price=signal.price or 0.0,
            confidence=signal.confidence,
            mode=mode,
            meta=signal.meta if signal.meta else {},
            trace=trace,
        )
        
    async def _fetch_history(self, symbol: str, timeframe: str, bars: int = 200) -> pd.DataFrame:
        """Fetch recent OHLC history from database."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT timestamp, open, high, low, close, volume
                FROM ohlc
                WHERE symbol = $1 AND timeframe = $2
                ORDER BY timestamp DESC
                LIMIT $3
            """, symbol, timeframe, bars)
            
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df
        
    async def _persist_signal(self, signal: Signal):
        """Persist signal to database."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO signals 
                (strategy_id, strategy_version, symbol, timeframe, side, 
                 price, confidence, meta, mode, idempotency_key, correlation_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'live', $9, $10)
            """,
            signal.strategy_id, signal.strategy_version, signal.symbol,
            signal.timeframe, signal.side, signal.price, signal.confidence,
            json.dumps(signal.meta), signal.idempotency_key, signal.correlation_id)
            
    async def shutdown(self):
        """Cleanup resources."""
        if self.execution_client:
            await self.execution_client.disconnect()
        if self.pool:
            await self.pool.close()
        logger.info("StrategyEngine shutdown")
