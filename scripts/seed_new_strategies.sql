-- Seed new strategies for signal-service (Assignment #4)
-- Run this SQL to add the 4 new strategies to the database

-- Volatility Expansion Strategy (Top performer: +15.3%)
INSERT INTO strategies (
    id, name, type, params, symbols, timeframes, version, mode, is_live, status, created_at
) VALUES (
    gen_random_uuid(),
    'volatility_expansion',
    'ta_lib',
    '{"keltner_len": 20, "atr_mult": 2.0, "bb_len": 20, "bb_mult": 2.0, "min_squeeze_bars": 3}'::jsonb,
    ARRAY['BTC', 'ETH', 'SOL', 'XRP', 'HYPER'],
    ARRAY['5m'],
    '1.0.0',
    'paper',
    true,
    'active',
    NOW()
) ON CONFLICT (name, version, mode) DO UPDATE SET
    params = EXCLUDED.params,
    symbols = EXCLUDED.symbols,
    timeframes = EXCLUDED.timeframes,
    is_live = EXCLUDED.is_live,
    status = EXCLUDED.status,
    updated_at = NOW();

-- Volume-Range Breakout Strategy (+13.1%)
INSERT INTO strategies (
    id, name, type, params, symbols, timeframes, version, mode, is_live, status, created_at
) VALUES (
    gen_random_uuid(),
    'volume_range_breakout',
    'ta_lib',
    '{"lookback": 20, "volume_threshold": 1.5, "min_range_pct": 0.3, "volatility_filter": 2.5}'::jsonb,
    ARRAY['BTC', 'ETH', 'SOL', 'XRP', 'HYPER'],
    ARRAY['5m'],
    '1.0.0',
    'paper',
    true,
    'active',
    NOW()
) ON CONFLICT (name, version, mode) DO UPDATE SET
    params = EXCLUDED.params,
    symbols = EXCLUDED.symbols,
    timeframes = EXCLUDED.timeframes,
    is_live = EXCLUDED.is_live,
    status = EXCLUDED.status,
    updated_at = NOW();

-- Momentum Strategy (+12.7%)
INSERT INTO strategies (
    id, name, type, params, symbols, timeframes, version, mode, is_live, status, created_at
) VALUES (
    gen_random_uuid(),
    'momentum',
    'ta_lib',
    '{"rsi_length": 14, "rsi_overbought": 65, "rsi_oversold": 35, "vwap_deviation": 0.5}'::jsonb,
    ARRAY['BTC', 'ETH', 'SOL', 'XRP', 'HYPER'],
    ARRAY['5m'],
    '1.0.0',
    'paper',
    true,
    'active',
    NOW()
) ON CONFLICT (name, version, mode) DO UPDATE SET
    params = EXCLUDED.params,
    symbols = EXCLUDED.symbols,
    timeframes = EXCLUDED.timeframes,
    is_live = EXCLUDED.is_live,
    status = EXCLUDED.status,
    updated_at = NOW();

-- ATR Breakout Strategy (+12.5%)
INSERT INTO strategies (
    id, name, type, params, symbols, timeframes, version, mode, is_live, status, created_at
) VALUES (
    gen_random_uuid(),
    'atr_breakout',
    'ta_lib',
    '{"atr_length": 14, "atr_mult": 1.5, "ema_filter": 50}'::jsonb,
    ARRAY['BTC', 'ETH', 'SOL', 'XRP', 'HYPER'],
    ARRAY['5m'],
    '1.0.0',
    'paper',
    true,
    'active',
    NOW()
) ON CONFLICT (name, version, mode) DO UPDATE SET
    params = EXCLUDED.params,
    symbols = EXCLUDED.symbols,
    timeframes = EXCLUDED.timeframes,
    is_live = EXCLUDED.is_live,
    status = EXCLUDED.status,
    updated_at = NOW();

-- Verify the strategies were inserted
SELECT name, version, mode, is_live, status, symbols 
FROM strategies 
WHERE name IN ('volatility_expansion', 'volume_range_breakout', 'momentum', 'atr_breakout')
ORDER BY name;
