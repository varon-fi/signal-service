"""Configuration settings."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Signal Service configuration."""
    
    database_url: str
    dataservice_addr: str
    signalservice_port: int
    executionservice_addr: str = "localhost:50053"
    trading_mode: str = "live"
    
    # OHLC subscription defaults
    default_symbols: list[str] = None
    default_timeframes: list[str] = None
    
    def __post_init__(self):
        if self.default_symbols is None:
            object.__setattr__(self, 'default_symbols', ['BTC', 'ETH', 'SOL', 'XRP', 'HYPER'])
        if self.default_timeframes is None:
            object.__setattr__(self, 'default_timeframes', ['5m'])
        object.__setattr__(self, 'trading_mode', self.trading_mode.lower())
