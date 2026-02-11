"""Strategy engine - loads and executes strategies."""

import json
from typing import Optional

import asyncpg
import pandas as pd
from structlog import get_logger

from signal_service.strategy.base import BaseStrategy, Signal

logger = get_logger(__name__)


class StrategyEngine:
    """Manages live strategies and generates signals."""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
        self.strategies: dict[str, BaseStrategy] = {}
        
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
                await self._persist_signal(signal)
                logger.info("Signal generated", 
                          strategy=strategy.name, 
                          symbol=symbol, 
                          side=signal.side)
                return signal
                
        return None
        
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
        if self.pool:
            await self.pool.close()
        logger.info("StrategyEngine shutdown")
