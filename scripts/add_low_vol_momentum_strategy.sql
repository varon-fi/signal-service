-- Add Low Volatility Momentum Strategy to database
-- Run this against the postgres database

INSERT INTO strategies (
    name,
    version,
    status,
    mode,
    symbols,
    timeframes,
    params,
    init_periods
) VALUES (
    'low_vol_momentum',
    '1.0.0',
    'active',
    'paper',  -- Start with paper trading
    ARRAY['BTC', 'ETH', 'SOL', 'ARB', 'OP', 'LINK'],
    ARRAY['15m'],
    '{
        "atr_period": 14,
        "lookback_days": 30,
        "low_vol_threshold": 40,
        "momentum_lookback": 48,
        "stop_loss_pct": 2.0
    }'::jsonb,
    500  -- Need 500 bars for warmup (30 days of 15m data)
);

-- Verify insertion
SELECT id, name, version, status, mode, symbols, timeframes 
FROM strategies 
WHERE name = 'low_vol_momentum';
