# Tuning Set I — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Runtime-aligned baseline (post-#45)

Authoritative baseline checkpoint:
- https://github.com/varon-fi/orders-service/issues/95#issuecomment-3979850555

Baseline entry-lane metrics (fixed epoch `2026-02-28T12:14:47Z` -> `2026-03-01T12:14:47Z`):
- `entry_match_rate`: **37.50%** (`3/8`)
- `entry_signal_delta_pct`: **12.50%**
- `entry_avg_price_delta_bps`: **6.56**
- `entry_side_agreement`: **100.00%**

## Tuning Set I sweep (single variable, exits unchanged)

Sweep axis:
- `ema_filter_period in [8, 10, 12]`
- fixed epoch locked to baseline window above
- all non-EMA controls fixed (`rsi_period=10`, `rsi_oversold=30`, `rsi_overbought=70`, `deviation_pct=0.9`)

Results:

| ema_filter_period | expected | actual_success | matched | entry_match_rate | entry_signal_delta_pct | entry_avg_price_delta_bps | entry_side_agreement |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 8 | 7 | 2 | 25.00% | 12.50% | **3.60** | 100.00% |
| 10 (baseline) | 8 | 7 | 3 | **37.50%** | **12.50%** | 6.56 | 100.00% |
| 12 | 9 | 7 | 3 | 33.33% | 22.22% | 6.56 | 100.00% |

Selected candidate:
- `ema_filter_period: 10 -> 8`

Selection rationale:
- blocker metric (`entry_signal_delta_pct`) is tied between `ema=8` and baseline `ema=10` at `12.50%`.
- `ema=8` is selected as deterministic tie-break candidate for lower price-drift (`entry_avg_price_delta_bps: 6.56 -> 3.60`) while preserving side-agreement guardrail.
- `ema=12` regresses blocker metric.

## Before/after (baseline -> Set I candidate)

| metric | baseline (`ema=10`) | Set I candidate (`ema=8`) | delta |
|---|---:|---:|---:|
| `entry_signal_delta_pct` | 12.50% | 12.50% | 0.00pp |
| `entry_match_rate` | 37.50% | 25.00% | -12.50pp |
| `entry_avg_price_delta_bps` | 6.56 | **3.60** | **-2.96** |
| `entry_side_agreement` | 100.00% | 100.00% | 0.00pp |

Expected effect hypothesis:
- preserve blocker metric while reducing price drift; requires merged runtime-aligned post-merge rerun on #95 to confirm whether live cadence variance changes blocker outcome.

## Artifacts
- `/tmp/tuning_set_i_ema8.json`
- `/tmp/tuning_set_i_ema10.json`
- `/tmp/tuning_set_i_ema12.json`
