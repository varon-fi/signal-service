"""Mock strategy engine for E2E testing without database."""

from typing import Optional
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from structlog import get_logger
from google.protobuf.timestamp_pb2 import Timestamp
import uuid

from signal_service.strategy.base import BaseStrategy, Signal
from signal_service.strategy.mtf_confluence import MtfConfluenceStrategy
from signal_service.grpc.execution_client import ExecutionServiceClient
from varon_fi.proto.varon_fi_pb2 import TradeSignal, TradingMode, TraceContext

logger = get_logger(__name__)


class MockStrategyEngine:
    """Mock strategy engine that runs strategies in-memory without DB.
    
    This is used for E2E testing when no database is available.
    """
    
    def __init__(self, execution_client: Optional[ExecutionServiceClient] = None):
        self.strategies: dict[str, BaseStrategy] = {}
        self.execution_client = execution_client
        self._history: dict[tuple[str, str], list] = {}
        
    def add_strategy(self, strategy: BaseStrategy):
        """Add a strategy to the engine."""
        self.strategies[strategy.strategy_id] = strategy
        
    async def initialize(self):
        """Initialize with default strategies (no DB required)."""
        # Create a default MTF Confluence strategy for testing
        default_strategy = MtfConfluenceStrategy(
            strategy_id="test-mtf-001",
            name="mtf_confluence",
            version="1.0.0",
            symbols=["BTC", "ETH"],
            timeframes=["5m"],
            params={
                "ema_len": 20,
                "rsi_len": 14,
                "rsi_overbought": 70,
                "rsi_oversold": 30,
                "atr_len": 14,
                "min_confidence": 0.6,
            },
        )
        self.strategies[default_strategy.strategy_id] = default_strategy
        logger.info("MockStrategyEngine initialized", strategy_count=len(self.strategies))
        
    async def connect_execution_service(self, addr: str):
        """Connect to ExecutionService for signal forwarding."""
        self.execution_client = ExecutionServiceClient(addr)
        await self.execution_client.connect()
        logger.info("ExecutionService client connected", addr=addr)
        
    async def process_candle(self, ohlc: dict) -> Optional[Signal]:
        """Process an OHLC candle and return signal if generated."""
        symbol = ohlc.get('symbol')
        timeframe = ohlc.get('timeframe')
        
        # Store in history
        key = (symbol, timeframe)
        if key not in self._history:
            self._history[key] = []
        self._history[key].append(ohlc)
        # Keep only last 200 bars
        self._history[key] = self._history[key][-200:]
        
        for strategy_id, strategy in self.strategies.items():
            if symbol not in strategy.symbols or timeframe not in strategy.timeframes:
                continue
                
            # Build history DataFrame
            history = self._get_history_dataframe(key)
            
            signal = strategy.on_candle(ohlc, history)
            if signal:
                signal.strategy_id = strategy_id
                signal.strategy_version = strategy.version
                signal.symbol = symbol
                signal.timeframe = timeframe
                
                # Send to ExecutionService if connected
                if self.execution_client:
                    try:
                        trade_signal = self._to_trade_signal(signal)
                        await self.execution_client.execute_signal(trade_signal)
                    except Exception as e:
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
        
    def _get_history_dataframe(self, key: tuple) -> pd.DataFrame:
        """Convert stored history to DataFrame."""
        bars = self._history.get(key, [])
        if not bars:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        df = pd.DataFrame(bars)
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp').reset_index(drop=True)
        return df
        
    def _to_trade_signal(self, signal: Signal) -> TradeSignal:
        """Convert internal Signal to TradeSignal protobuf."""
        now = datetime.now(timezone.utc)
        timestamp = Timestamp()
        timestamp.FromDatetime(now)

        mode_str = (signal.meta.get("mode", "paper") if signal.meta else "paper").lower()
        mode = TradingMode.LIVE if mode_str == "live" else TradingMode.PAPER

        trace = TraceContext(
            correlation_id=signal.correlation_id if signal.correlation_id else str(uuid.uuid4()),
            idempotency_key=signal.idempotency_key if signal.idempotency_key else str(uuid.uuid4()),
            source_service="signal-service",
            latency_ms=0,
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
        
    async def shutdown(self):
        """Cleanup resources."""
        if self.execution_client:
            await self.execution_client.disconnect()
        logger.info("MockStrategyEngine shutdown")
