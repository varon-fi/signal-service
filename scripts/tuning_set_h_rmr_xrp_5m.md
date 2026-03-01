# Tuning Set H — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Runtime-aligned baseline (post-#44)

Authoritative baseline checkpoint:
- https://github.com/varon-fi/orders-service/issues/95#issuecomment-3979765557

Baseline entry-lane metrics (fixed epoch `2026-02-28T11:11:34Z` -> `2026-03-01T11:11:34Z`):
- `entry_match_rate`: **27.27%** (`3/11`)
- `entry_signal_delta_pct`: **36.36%**
- `entry_avg_price_delta_bps`: **6.56**
- `entry_side_agreement`: **100.00%**

## Tuning Set H sweep (single variable, exits unchanged)

Sweep axis:
- `ema_filter_period in [10, 15, 20]`
- fixed epoch locked to baseline window above
- all non-EMA controls fixed (`rsi_period=10`, `rsi_oversold=30`, `rsi_overbought=70`, `deviation_pct=0.9`)

Results:

| ema_filter_period | expected | actual_success | matched | entry_match_rate | entry_signal_delta_pct | entry_avg_price_delta_bps | entry_side_agreement |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 8 | 7 | 3 | **37.50%** | **12.50%** | **6.56** | 100.00% |
| 15 (baseline) | 11 | 7 | 3 | 27.27% | 36.36% | 6.56 | 100.00% |
| 20 | 11 | 7 | 3 | 27.27% | 36.36% | 6.56 | 100.00% |

Selected candidate:
- `ema_filter_period: 15 -> 10`

Selection rationale:
- `ema=10` is the only point that materially improves blocker metrics while preserving guardrails.
- `ema=10` improves both cadence mismatch and match-rate without price/side regressions.

## Before/after (baseline -> Set H candidate)

| metric | baseline (`ema=15`) | Set H candidate (`ema=10`) | delta |
|---|---:|---:|---:|
| `entry_signal_delta_pct` | 36.36% | **12.50%** | **-23.86pp** |
| `entry_match_rate` | 27.27% | **37.50%** | **+10.23pp** |
| `entry_avg_price_delta_bps` | 6.56 | 6.56 | 0.00 |
| `entry_side_agreement` | 100.00% | 100.00% | 0.00pp |

Expected effect hypothesis:
- materially reduce entry cadence mismatch while preserving price/side guardrails.
- gate may still remain BLOCK until `entry_signal_delta_pct <= 5.00` is reached, but Set H should move the blocker metric closer to threshold.

## Repro commands

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

# Set H sweep point: ema_filter_period=10
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_h_ema10.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T11:11:34Z --end 2026-03-01T11:11:34Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":10,"max_atr_pct":3.0,"vwap_tolerance":0.0015,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_h_ema10.json \
  --out-md /tmp/tuning_set_h_ema10.md
```

## Artifacts
- `/tmp/tuning_set_h_ema10.json`
- `/tmp/tuning_set_h_ema15.json`
- `/tmp/tuning_set_h_ema20.json`

## Gate implication

Expected direction after applying Set H:
- meaningful improvement in the primary blocker metric (`entry_signal_delta_pct`) with preserved guardrails.
- requires post-merge runtime-aligned rerun on `orders-service#95` to validate persistence.
