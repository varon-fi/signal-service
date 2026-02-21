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
    ):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
        self.strategies: dict[str, BaseStrategy] = {}
        self.execution_client = execution_client
        # Track last processed candle per strategy/symbol/timeframe to prevent duplicate signals
        self._last_candle_ts: dict[str, datetime] = {}
        # Track warmup requirements per strategy/symbol/timeframe
        self._warmup_required: dict[str, int] = {}
        self._warmup_complete: dict[str, bool] = {}
        # Track latest candle timestamp seen at startup per symbol/timeframe
        self._startup_latest_ts: dict[str, datetime] = {}
        
    async def connect_execution_service(self, addr: str):
        """Connect to ExecutionService for signal forwarding."""
        self.execution_client = ExecutionServiceClient(addr)
        await self.execution_client.connect()
        logger.info("ExecutionService client connected", addr=addr)
        
    async def initialize(self):
        """Initialize DB connection and load active strategies."""
        self.pool = await asyncpg.create_pool(self.database_url)
        await self._load_strategies()
        await self._initialize_startup_state()
        logger.info("StrategyEngine initialized", strategy_count=len(self.strategies))

    async def reload_strategies(self):
        """Reload strategies from the database."""
        self.strategies.clear()
        self._warmup_required.clear()
        self._warmup_complete.clear()
        await self._load_strategies()
        await self._initialize_startup_state()
        logger.info("Strategies reloaded", strategy_count=len(self.strategies))

    def get_required_subscriptions(self) -> dict[str, list[str]]:
        """Get all symbol/timeframe combinations required by active strategies.

        Returns:
            Dict mapping timeframe -> list of symbols
        """
        subscriptions: dict[str, set[str]] = {}
        for strategy in self.strategies.values():
            for timeframe in strategy.timeframes:
                if timeframe not in subscriptions:
                    subscriptions[timeframe] = set()
                subscriptions[timeframe].update(strategy.symbols)
        return {tf: list(symbols) for tf, symbols in subscriptions.items()}
        
    async def _load_strategies(self):
        """Load all active strategies from database."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT *
                FROM strategies
                WHERE status = 'active'
            """)
            
        for row in rows:
            strategy = self._create_strategy(row)
            if strategy:
                strategy_id = str(row['id'])
                self.strategies[strategy_id] = strategy
                # Initialize warmup tracking for each strategy/symbol/timeframe combo
                init_periods = row.get("init_periods") if isinstance(row, dict) else row.get("init_periods")
                for symbol in strategy.symbols:
                    for timeframe in strategy.timeframes:
                        warmup_key = f"{strategy_id}:{symbol}:{timeframe}"
                        required = int(init_periods) if init_periods else 0
                        self._warmup_required[warmup_key] = required
                        self._warmup_complete[warmup_key] = (required == 0)
                        logger.info("Strategy warmup initialized",
                                  strategy=strategy.name,
                                  symbol=symbol,
                                  timeframe=timeframe,
                                  min_bars=required)
                
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
            strategy = create_strategy(
                config,
                strategy_id=str(row["id"]),
                name=name,
                version=version,
                symbols=row["symbols"],
                timeframes=row["timeframes"],
            )
            mode_val = row.get("mode") if isinstance(row, dict) else row.get("mode")
            if mode_val:
                setattr(strategy, "mode", str(mode_val))
            return strategy
        except KeyError:
            logger.warning(
                "Unknown strategy type",
                name=name,
                available=list(list_strategies().keys()),
            )
            return None

    async def _initialize_startup_state(self):
        """Initialize startup gating and warmup state."""
        if not self.strategies:
            return

        # Build unique symbol/timeframe combinations across strategies
        combos: set[tuple[str, str]] = set()
        for strategy in self.strategies.values():
            for symbol in strategy.symbols:
                for timeframe in strategy.timeframes:
                    combos.add((symbol, timeframe))

        # Record latest candle timestamp at startup per symbol/timeframe
        async with self.pool.acquire() as conn:
            for symbol, timeframe in combos:
                row = await conn.fetchrow(
                    """
                    SELECT MAX(ts) as ts
                    FROM ohlcs o
                    JOIN instruments i ON o.instrument_id = i.id
                    WHERE i.symbol = $1 AND o.timeframe = $2
                    """,
                    symbol, timeframe
                )
                if row and row["ts"]:
                    self._startup_latest_ts[f"{symbol}:{timeframe}"] = row["ts"]

        # Prime warmup state by fetching historical bars
        for strategy_id, strategy in self.strategies.items():
            history_source = (strategy.params or {}).get("history_source", "ohlcs")
            lookback_days = (strategy.params or {}).get("lookback_days")
            for symbol in strategy.symbols:
                for timeframe in strategy.timeframes:
                    warmup_key = f"{strategy_id}:{symbol}:{timeframe}"
                    required = self._warmup_required.get(warmup_key, 0)
                    lookback_bars = self._calc_lookback_bars(timeframe, lookback_days)
                    bars_needed = max(required, lookback_bars) if lookback_bars else required
                    if bars_needed > 0:
                        history = await self._fetch_history(
                            symbol, timeframe, bars=bars_needed, source=history_source
                        )
                        if len(history) >= required:
                            self._warmup_complete[warmup_key] = True
                            logger.info("Strategy warmup complete",
                                        strategy=strategy.name,
                                        symbol=symbol,
                                        timeframe=timeframe,
                                        history_bars=len(history))
                        else:
                            self._warmup_complete[warmup_key] = False
                            logger.info("Strategy warmup pending",
                                        strategy=strategy.name,
                                        symbol=symbol,
                                        timeframe=timeframe,
                                        history_bars=len(history),
                                        required_bars=required)

    def _calc_lookback_bars(self, timeframe: str, lookback_days: Optional[int]) -> int:
        """Convert lookback_days into number of bars for a timeframe."""
        if not lookback_days or not timeframe:
            return 0
        tf = str(timeframe).strip().lower()
        try:
            if tf.endswith("m"):
                minutes = int(tf[:-1])
                if minutes <= 0:
                    return 0
                bars_per_day = int((24 * 60) / minutes)
                return int(lookback_days) * bars_per_day
            if tf.endswith("h"):
                hours = int(tf[:-1])
                if hours <= 0:
                    return 0
                bars_per_day = int(24 / hours)
                return int(lookback_days) * bars_per_day
            if tf.endswith("d"):
                days = int(tf[:-1]) if tf[:-1] else 1
                if days <= 0:
                    return 0
                return int(lookback_days / days)
        except Exception:
            return 0
        return 0

    def _in_strategy_session(self, strategy: BaseStrategy, candle_ts: Optional[datetime]) -> bool:
        """Check per-strategy session window if available."""
        if candle_ts is None:
            return True

        # Strategy-provided helper
        if hasattr(strategy, "_in_session") and callable(getattr(strategy, "_in_session")):
            try:
                return bool(strategy._in_session(candle_ts))
            except Exception:
                return True

        # Fallback to params or attributes
        params = getattr(strategy, "params", None) or {}
        session_start = params.get("session_start") or getattr(strategy, "session_start", None)
        session_end = params.get("session_end") or getattr(strategy, "session_end", None)
        if not session_start or not session_end:
            return True

        # Parse session times if given as strings ("HH:MM")
        if isinstance(session_start, str):
            session_start = datetime.strptime(session_start, "%H:%M").time()
        if isinstance(session_end, str):
            session_end = datetime.strptime(session_end, "%H:%M").time()

        current_time = candle_ts.time()
        return session_start <= current_time <= session_end
        
    async def process_candle(self, ohlc: dict) -> Optional[Signal]:
        """Process an OHLC candle and return signal if generated."""
        symbol = ohlc.get('symbol')
        timeframe = ohlc.get('timeframe')
        
        for strategy_id, strategy in self.strategies.items():
            if symbol not in strategy.symbols or timeframe not in strategy.timeframes:
                continue
            
            # Normalize candle timestamp
            candle_ts = self._normalize_candle_ts(ohlc.get('timestamp') or ohlc.get('ts'))

            # Per-strategy session window enforcement (requirement #1)
            if not self._in_strategy_session(strategy, candle_ts):
                continue

            # Live-candle gating: skip candles at or before startup latest ts (requirement #3)
            if candle_ts is not None:
                startup_key = f"{symbol}:{timeframe}"
                startup_ts = self._startup_latest_ts.get(startup_key)
                if startup_ts is not None and candle_ts <= startup_ts:
                    continue

            # De-duplicate per candle: only process each candle once per strategy/symbol/timeframe
            if candle_ts is not None:
                dedupe_key = f"{strategy_id}:{symbol}:{timeframe}"
                last_ts = self._last_candle_ts.get(dedupe_key)
                if last_ts is not None and candle_ts <= last_ts:
                    continue
                self._last_candle_ts[dedupe_key] = candle_ts

            # Fetch recent history for this symbol/timeframe
            warmup_key = f"{strategy_id}:{symbol}:{timeframe}"
            required = self._warmup_required.get(warmup_key, 0)
            history_source = (strategy.params or {}).get("history_source", "ohlcs")
            lookback_days = (strategy.params or {}).get("lookback_days")
            lookback_bars = self._calc_lookback_bars(timeframe, lookback_days)
            bars_needed = max(200, required, lookback_bars) if lookback_bars else max(200, required)
            history = await self._fetch_history(symbol, timeframe, bars=bars_needed, source=history_source)

            # Warmup check (requirement #2)
            if required > 0 and not self._warmup_complete.get(warmup_key, True):
                if len(history) < required:
                    logger.debug("Skipping signal - warmup in progress",
                               strategy=strategy.name, symbol=symbol, timeframe=timeframe,
                               history_bars=len(history), required_bars=required)
                    continue
                else:
                    self._warmup_complete[warmup_key] = True
                    logger.info("Strategy warmup complete",
                              strategy=strategy.name, symbol=symbol, timeframe=timeframe,
                              history_bars=len(history))
            
            signal = strategy.on_candle(ohlc, history)
            if signal:
                signal.strategy_id = strategy_id
                signal.strategy_version = strategy.version
                signal.symbol = symbol
                signal.timeframe = timeframe

                if signal.meta is None:
                    signal.meta = {}
                if getattr(strategy, "mode", None):
                    signal.meta["mode"] = getattr(strategy, "mode")
                
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

    def _normalize_candle_ts(self, ts) -> Optional[datetime]:
        """Normalize candle timestamp to timezone-aware UTC datetime."""
        if ts is None:
            return None

        # Handle different timestamp types
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts, timezone.utc)
        elif isinstance(ts, datetime):
            dt = ts
        elif hasattr(ts, 'seconds') and hasattr(ts, 'nanos'):
            dt = datetime.fromtimestamp(ts.seconds, timezone.utc)
        elif isinstance(ts, str):
            dt = pd.to_datetime(ts)
        elif hasattr(ts, 'ToDatetime'):
            dt = ts.ToDatetime(tzinfo=timezone.utc)
        else:
            return None

        # Convert pandas Timestamp to datetime if needed
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()

        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt
        
    def _to_trade_signal(self, signal: Signal) -> TradeSignal:
        """Convert internal Signal to TradeSignal protobuf matching PR#7 proto."""
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        # Convert mode string to TradingMode enum
        mode_str = (signal.meta.get("mode") if signal.meta else None) or "paper"
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
        
    async def _fetch_history(self, symbol: str, timeframe: str, bars: int = 200, source: str = "ohlcs") -> pd.DataFrame:
        """Fetch recent OHLC history from database for symbol and timeframe.

        Args:
            source: "ohlcs" (default) or "imported" (ohlc_imports table)
        """
        async with self.pool.acquire() as conn:
            if source == "imported":
                rows = await conn.fetch("""
                    SELECT ts as timestamp, open, high, low, close, volume
                    FROM ohlc_imports o
                    JOIN instruments i ON o.instrument_id = i.id
                    WHERE i.symbol = $1 AND o.timeframe = $2
                    ORDER BY ts DESC
                    LIMIT $3
                """, symbol, timeframe, bars)

                # Fallback to regular ohlcs view if imported data is missing
                if not rows:
                    rows = await conn.fetch("""
                        SELECT ts as timestamp, open, high, low, close, volume
                        FROM ohlcs o
                        JOIN instruments i ON o.instrument_id = i.id
                        WHERE i.symbol = $1 AND o.timeframe = $2
                        ORDER BY ts DESC
                        LIMIT $3
                    """, symbol, timeframe, bars)
            else:
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
            
            mode_val = (signal.meta.get("mode") if signal.meta else None) or "paper"
            await conn.execute("""
                INSERT INTO signals 
                (exchange_id, instrument_id, strategy_id, strategy_version,
                 signal_type, signal_value, confidence, payload, mode, 
                 idempotency_key, correlation_id)
                VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            instrument_id, signal.strategy_id, signal.strategy_version,
            signal_type, signal_value, signal.confidence,
            json.dumps(signal.meta), mode_val, 
            signal.idempotency_key, signal.correlation_id)
            
    async def shutdown(self):
        """Cleanup resources."""
        if self.execution_client:
            await self.execution_client.disconnect()
        if self.pool:
            await self.pool.close()
        logger.info("StrategyEngine shutdown")
