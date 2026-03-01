# Tuning Set G — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Runtime-aligned baseline (Set F)

Authoritative baseline checkpoint:
- https://github.com/varon-fi/orders-service/issues/95#issuecomment-3979712971

Baseline entry-lane metrics (fixed epoch `2026-02-28T10:33:30Z` -> `2026-03-01T10:33:30Z`):
- `entry_match_rate`: **27.27%** (`3/11`)
- `entry_signal_delta_pct`: **36.36%**
- `entry_avg_price_delta_bps`: **6.56**
- `entry_side_agreement`: **100.00%**

## Tuning Set G sweep (single variable, exits unchanged)

Sweep axis:
- `ema_filter_period in [15, 20, 25]`
- fixed epoch locked to baseline window above
- all non-EMA controls fixed (`rsi_period=10`, `rsi_oversold=30`, `rsi_overbought=70`, `deviation_pct=0.9`)

Results:

| ema_filter_period | expected | actual_success | matched | entry_match_rate | entry_signal_delta_pct | entry_avg_price_delta_bps | entry_side_agreement |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 15 | 11 | 7 | 3 | 27.27% | **36.36%** | **6.56** | 100.00% |
| 20 (baseline) | 11 | 7 | 3 | 27.27% | **36.36%** | **6.56** | 100.00% |
| 25 | 14 | 7 | 4 | 28.57% | 50.00% | 7.63 | 100.00% |

Selected candidate:
- `ema_filter_period: 20 -> 15`

Selection rationale:
- `ema=25` regresses the primary blocker metric (`entry_signal_delta_pct`).
- `ema=15` ties `ema=20` on fixed-epoch gate metrics while preserving both guardrails (`entry_avg_price_delta_bps <= 15`, `entry_side_agreement >= 99%`).
- With tied baseline metrics, choose the lower EMA as deterministic tie-breaker for the next live-cadence validation step.

## Before/after (Set F baseline -> Set G candidate)

| metric | Set F baseline (`ema=20`) | Set G candidate (`ema=15`) | delta |
|---|---:|---:|---:|
| `entry_signal_delta_pct` | 36.36% | 36.36% | 0.00pp |
| `entry_match_rate` | 27.27% | 27.27% | 0.00pp |
| `entry_avg_price_delta_bps` | 6.56 | 6.56 | 0.00 |
| `entry_side_agreement` | 100.00% | 100.00% | 0.00pp |

Expected effect hypothesis:
- Maintain current fixed-epoch parity quality (no regressions in price/side guardrails).
- Test whether a slightly faster entry filter (`ema=15`) improves live entry cadence without degrading match quality.

## Repro commands

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

# Set G sweep point: ema_filter_period=15
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_g_ema15.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T10:33:30Z --end 2026-03-01T10:33:30Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":15,"max_atr_pct":3.0,"vwap_tolerance":0.0015,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_g_ema15.json \
  --out-md /tmp/tuning_set_g_ema15.md
```

## Artifacts
- `/tmp/tuning_set_g_ema15.json`
- `/tmp/tuning_set_g_ema20.json`
- `/tmp/tuning_set_g_ema25.json`

## Gate implication

Expected direction after applying Set G:
- preserve current entry delta while attempting to improve live cadence responsiveness.
- gate likely remains BLOCK until post-apply fixed-epoch rerun on `#95` confirms any measurable entry-lane gain.
