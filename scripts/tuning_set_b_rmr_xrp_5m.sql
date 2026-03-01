-- Tuning Set B candidate for XRP 5m range_mean_reversion
-- Issue: https://github.com/varon-fi/signal-service/issues/39
-- Strategy id: 236d3378-1be5-4264-ac97-79c9d0dbaf12
--
-- Intent:
-- - tighten entry emission cadence toward runtime-observed flow
-- - target entry timing alignment (count + match-rate) while preserving side agreement
--
-- Candidate parameter delta (from deployed baseline):
-- - rsi_period: 14 -> 10
-- - rsi_oversold: 34 -> 30
-- - rsi_overbought: 66 -> 70
-- - keep deviation_pct=0.9, ema_filter_period=100, max_atr_pct=3.0

BEGIN;

UPDATE strategies
SET params = jsonb_build_object(
    'vwap_lookback', 20,
    'rsi_period', 10,
    'rsi_oversold', 30,
    'rsi_overbought', 70,
    'deviation_pct', 0.9,
    'ema_filter_period', 100,
    'max_atr_pct', 3.0,
    'vwap_tolerance', 0.0018,
    'max_hold_candles', 15,
    'stop_loss_enabled', true,
    'stop_loss_multiplier', 1.5
),
updated_at = now()
WHERE id = '236d3378-1be5-4264-ac97-79c9d0dbaf12'
  AND name = 'range_mean_reversion'
  AND mode = 'paper';

-- Optional verification snippet (run after COMMIT):
-- SELECT id, name, mode, params, updated_at
-- FROM strategies
-- WHERE id = '236d3378-1be5-4264-ac97-79c9d0dbaf12';

COMMIT;
