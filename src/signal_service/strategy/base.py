"""Base strategy interface."""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd


@dataclass
class Signal:
    """A trading signal."""
    side: str  # 'long' or 'short'
    price: Optional[float] = None
    confidence: float = 0.5
    meta: dict = field(default_factory=dict)
    
    # Populated by engine
    strategy_id: Optional[str] = None
    strategy_version: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class BaseStrategy(ABC):
    """Abstract base class for trading strategies."""
    
    def __init__(
        self,
        strategy_id: str,
        name: str,
        version: str,
        symbols: list[str],
        timeframes: list[str],
        params: dict[str, Any],
    ):
        self.strategy_id = strategy_id
        self.name = name
        self.version = version
        self.symbols = symbols
        self.timeframes = timeframes
        self.params = params
        
    @abstractmethod
    def on_candle(self, candle: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process a new candle and optionally return a signal."""
        pass
        
    def on_tick(self, tick: dict, history: pd.DataFrame) -> Optional[Signal]:
        """Process a tick update (for tick-based strategies). Override if needed."""
        return None
        
    def get_state(self) -> dict:
        """Serialize strategy state for recovery. Override if stateful."""
        return {}
        
    def set_state(self, state: dict):
        """Restore strategy state. Override if stateful."""
        pass
