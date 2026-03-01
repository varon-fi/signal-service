# Tuning Set F — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Runtime-aligned baseline (Set E)

Canonical runtime-aligned checkpoint:
- https://github.com/varon-fi/orders-service/issues/95#issuecomment-3979570579

Baseline entry-lane metrics (fixed epoch `2026-02-28T09:26:53Z` -> `2026-03-01T09:26:53Z`):
- `entry_match_rate`: **31.25%** (`5/16`)
- `entry_signal_delta_pct`: **56.25%**
- `entry_avg_price_delta_bps`: **11.32**
- `entry_side_agreement`: **100.00%**

## Tuning Set F sweep (single variable, exits unchanged)

Sweep axis:
- `ema_filter_period in [20, 30, 50]`
- fixed epoch locked to baseline window above
- all non-EMA controls fixed (`rsi_period=10`, `rsi_oversold=30`, `rsi_overbought=70`, `deviation_pct=0.9`)

Results:

| ema_filter_period | expected | actual_success | matched | entry_match_rate | entry_signal_delta_pct | entry_avg_price_delta_bps | entry_side_agreement |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 | 11 | 7 | 3 | 27.27% | **36.36%** | **6.56** | 100.00% |
| 30 (baseline) | 16 | 7 | 5 | 31.25% | 56.25% | 11.32 | 100.00% |
| 50 | 23 | 7 | 6 | 26.09% | 69.57% | 12.74 | 100.00% |

Selected candidate:
- `ema_filter_period: 30 -> 20`

## Before/after (Set E baseline -> Set F candidate)

| metric | Set E baseline (`ema=30`) | Set F candidate (`ema=20`) | delta |
|---|---:|---:|---:|
| `entry_signal_delta_pct` | 56.25% | **36.36%** | **-19.89pp** |
| `entry_match_rate` | 31.25% | 27.27% | -3.98pp |
| `entry_avg_price_delta_bps` | 11.32 | **6.56** | -4.76 |
| `entry_side_agreement` | 100.00% | 100.00% | 0.00pp |

Interpretation:
- Candidate materially improves the primary failing metric (`entry_signal_delta_pct`) while preserving price/side safety thresholds.
- Match-rate tradeoff exists, but candidate still reduces expected-only mismatch burden by lowering cadence sensitivity and tightening price drift.

## Repro command

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

# Set F sweep point: ema_filter_period=20
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_f_ema20.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T09:26:53Z --end 2026-03-01T09:26:53Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":20,"max_atr_pct":3.0,"vwap_tolerance":0.0015,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_f_ema20.json \
  --out-md /tmp/tuning_set_f_ema20.md
```

## Artifacts
- `/tmp/tuning_set_f_ema20.json`
- `/tmp/tuning_set_f_ema30.json`
- `/tmp/tuning_set_f_ema50.json`

## Gate implication

Expected direction after applying Set F:
- lower entry signal delta with preserved side and stronger price alignment.
- gate likely remains BLOCK until live post-apply fixed-epoch rerun on `#95` confirms persisted improvement.
