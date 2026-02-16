"""Strategy engine - loads and executes strategies."""

import json
import uuid
from typing import Optional

import asyncpg
import pandas as pd
from structlog import get_logger
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone

import signal_service.strategy  # registers built-in strategies
from varon_fi import BaseStrategy, Signal, StrategyConfig, create_strategy, list_strategies
from signal_service.grpc.execution_client import ExecutionServiceClient
from varon_fi.proto.varon_fi_pb2 import TradeSignal, TradingMode, TraceContext
logger = get_logger(__name__)


class StrategyEngine:
    """Manages live strategies and generates signals."""
    
    def __init__(
        self,
        database_url: str,
        execution_client: Optional[ExecutionServiceClient] = None,
        mode: str = "live",
    ):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
        self.strategies: dict[str, BaseStrategy] = {}
        self.execution_client = execution_client
        self.mode = mode.lower()
        self._last_signal_time: dict[str, datetime] = {}  # Cooldown tracking per strategy-symbol
        self.signal_cooldown_minutes = 15  # Minimum minutes between signals from same strategy-symbol
        
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

    async def reload_strategies(self):
        """Reload strategies from the database."""
        self.strategies.clear()
        await self._load_strategies()
        logger.info("Strategies reloaded", strategy_count=len(self.strategies))
        
    async def _load_strategies(self):
        """Load active strategies from database for the configured mode."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, name, type, params, symbols, timeframes, version, mode, is_live, status
                FROM strategies
                WHERE is_live = true AND status = 'active' AND mode = $1
            """, self.mode)
            
        for row in rows:
            strategy = self._create_strategy(row)
            if strategy:
                self.strategies[str(row['id'])] = strategy
                
    def _create_strategy(self, row: asyncpg.Record) -> Optional[BaseStrategy]:
        """Instantiate a strategy from DB row."""
        name = row.get("name") if isinstance(row, dict) else row["name"]
        if not name:
            logger.warning("Strategy missing name", row=row)
            return None

        raw_params = row.get("params") if isinstance(row, dict) else row["params"]
        if raw_params is None:
            logger.warning("Strategy missing params; defaulting to empty", name=name)
            params = {}
        else:
            params = json.loads(raw_params) if isinstance(raw_params, str) else raw_params

        version = row.get("version") if isinstance(row, dict) else row["version"]
        if not version:
            logger.warning("Strategy missing version; defaulting to 1.0.0", name=name)
            version = "1.0.0"

        config = StrategyConfig(name=name, params=params)
        try:
            return create_strategy(
                config,
                strategy_id=str(row["id"]),
                name=name,
                version=version,
                symbols=row["symbols"],
                timeframes=row["timeframes"],
            )
        except KeyError:
            logger.warning(
                "Unknown strategy type",
                name=name,
                available=list(list_strategies().keys()),
            )
            return None
        
    async def process_candle(self, ohlc: dict) -> Optional[Signal]:
        """Process an OHLC candle and return signal if generated."""
        symbol = ohlc.get('symbol')
        timeframe = ohlc.get('timeframe')
        
        for strategy_id, strategy in self.strategies.items():
            if symbol not in strategy.symbols or timeframe not in strategy.timeframes:
                continue
            
            # Check cooldown - prevent signal spam
            cooldown_key = f"{strategy_id}:{symbol}"
            now = datetime.now(timezone.utc)
            last_time = self._last_signal_time.get(cooldown_key)
            if last_time and (now - last_time).total_seconds() < (self.signal_cooldown_minutes * 60):
                continue  # Still in cooldown period
                
            # Fetch recent history for this symbol/timeframe
            history = await self._fetch_history(symbol, timeframe, bars=200)
            
            signal = strategy.on_candle(ohlc, history)
            if signal:
                signal.strategy_id = strategy_id
                signal.strategy_version = strategy.version
                signal.symbol = symbol
                signal.timeframe = timeframe
                
                # Update cooldown tracker
                self._last_signal_time[cooldown_key] = now
                
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
        mode_str = (signal.meta.get("mode") if signal.meta else None) or self.mode
        mode_str = str(mode_str).lower()
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
            meta={str(k): "" if v is None else str(v) for k, v in signal.meta.items()} if signal.meta else {},
            trace=trace,
        )
        
    async def _fetch_history(self, symbol: str, timeframe: str, bars: int = 200) -> pd.DataFrame:
        """Fetch recent OHLC history from database for symbol and timeframe."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT ts as timestamp, open, high, low, close, volume
                FROM ohlcs o
                JOIN instruments i ON o.instrument_id = i.id
                WHERE i.symbol = $1 AND o.timeframe = $2
                ORDER BY ts DESC
                LIMIT $3
            """, symbol, timeframe, bars)
            
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df
        
    async def _persist_signal(self, signal: Signal):
        """Persist signal to database."""
        async with self.pool.acquire() as conn:
            # Map symbol to instrument_id (Hyperliquid = exchange_id 1)
            instrument_id = await conn.fetchval("""
                SELECT id FROM instruments WHERE symbol = $1
            """, signal.symbol)
            
            if instrument_id is None:
                logger.warning("Unknown instrument for signal", symbol=signal.symbol)
                return
            
            # Map side to signal_type/signal_value
            signal_type = signal.side.upper() if signal.side else "UNKNOWN"
            signal_value = float(signal.price) if signal.price else 0.0
            
            await conn.execute("""
                INSERT INTO signals 
                (exchange_id, instrument_id, strategy_id, strategy_version,
                 signal_type, signal_value, confidence, payload, mode, 
                 idempotency_key, correlation_id)
                VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            instrument_id, signal.strategy_id, signal.strategy_version,
            signal_type, signal_value, signal.confidence,
            json.dumps(signal.meta), self.mode, 
            signal.idempotency_key, signal.correlation_id)
            
    async def shutdown(self):
        """Cleanup resources."""
        if self.execution_client:
            await self.execution_client.disconnect()
        if self.pool:
            await self.pool.close()
        logger.info("StrategyEngine shutdown")
