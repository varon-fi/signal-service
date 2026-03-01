# Tuning Set E — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Authoritative baseline

Pinned source checkpoint (normalized):
- https://github.com/varon-fi/orders-service/issues/95#issuecomment-3979511679

Baseline entry-lane metrics (fixed epoch `2026-02-28T08:46:59Z` -> `2026-03-01T08:46:59Z`):
- `entry_match_rate`: **27.27%** (`6/22`)
- `entry_signal_delta_pct`: **63.64%**
- `entry_avg_price_delta_bps`: **12.74**
- `entry_side_agreement`: **100.00%**

## Timing-mismatch attribution pass (same epoch)

Method:
- paired expected vs actual entry signals with 1-candle tolerance (5m).
- for `expected_only` misses, computed nearest actual timestamp (unbounded) and bucketed:
  - nearest-candle offset (`round((nearest_actual_ts - expected_ts)/300s)`)
  - minute-of-hour of expected miss
- reported absolute timing deltas for matched vs misses.

Attribution highlights:
- Matched abs delta: `p50=2.90s`, `p95=299.04s`
- Miss abs delta: `p50=597.10s`, `p95=1801.13s`
- Miss offset-candles buckets:
  - `{-6:1, -5:2, -4:2, -2:2, -1:2, +1:5, +6:1, +9:1}`
- Miss minute-of-hour concentration:
  - `45` (4 misses), `30` (3 misses), `35` (2 misses), remainder singleton buckets

Interpretation:
- Misses cluster in ±1 to ±6+ candle offsets while matched events are tightly near-candle, consistent with entry-cadence sensitivity rather than side-direction disagreement.

## Single-variable control delta selected

From attribution + single-variable sweeps, selected one entry-timing control:
- `ema_filter_period: 100 -> 30`
- exits unchanged

## Fixed-epoch before/after evidence (single-variable)

| metric | baseline (`ema_filter_period=100`) | candidate (`ema_filter_period=30`) | delta |
|---|---:|---:|---:|
| `entry_signal_delta_pct` | 63.64% | 50.00% | **-13.64pp** |
| `entry_match_rate` | 27.27% | 31.25% | +3.98pp |
| `entry_avg_price_delta_bps` | 12.74 | 11.32 | -1.42 |
| `entry_side_agreement` | 100.00% | 100.00% | 0.00pp |

PR-go criterion check for this pass:
- expected `entry_signal_delta_pct` reduction >=10pp: ✅ (`13.64pp`)
- preserve `entry_avg_price_delta_bps <= 15`: ✅ (`11.32`)
- preserve `entry_side_agreement >= 99%`: ✅ (`100%`)

## Repro commands

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

# baseline (authoritative)
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_c_runtime.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T08:46:59Z --end 2026-03-01T08:46:59Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":100,"max_atr_pct":3.0,"vwap_tolerance":0.0015,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_e_before.json \
  --out-md /tmp/tuning_set_e_before.md

# candidate (single-variable delta)
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_e_ema30.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T08:46:59Z --end 2026-03-01T08:46:59Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":30,"max_atr_pct":3.0,"vwap_tolerance":0.0015,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_e_after_ema30.json \
  --out-md /tmp/tuning_set_e_after_ema30.md
```

## Artifacts
- `/tmp/timing_attribution_baseline_vs_ema30.json`
- `/tmp/tuning_set_e_before.json`
- `/tmp/tuning_set_e_after_ema30.json`

## Gate implication

Expected post-merge direction: significant reduction in entry signal-delta with thresholds preserved on price/side, but gate likely remains BLOCK until post-merge runtime rerun on `#95` confirms live alignment.
