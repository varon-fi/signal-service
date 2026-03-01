# Tuning Set B — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Candidate params

```json
{
  "vwap_lookback": 20,
  "rsi_period": 10,
  "rsi_oversold": 30,
  "rsi_overbought": 70,
  "deviation_pct": 0.9,
  "ema_filter_period": 100,
  "max_atr_pct": 3.0,
  "vwap_tolerance": 0.0018,
  "max_hold_candles": 15,
  "stop_loss_enabled": true,
  "stop_loss_multiplier": 1.5
}
```

## Fixed-epoch parity evidence (same 24h window)

Epoch (locked): `2026-02-28T03:35:47Z` → `2026-03-01T03:35:47Z`

### Baseline (deployed params)
- `entry_match_rate`: **19.35%** (`6/31`)
- `entry_signal_delta_pct`: **58.06%**
- `entry_avg_price_delta_bps`: **27.47**
- `entry_side_agreement`: **100.00%**

### Tuning Set B candidate (same epoch)
- `entry_match_rate`: **27.27%** (`6/22`)
- `entry_signal_delta_pct`: **40.91%**
- `entry_avg_price_delta_bps`: **27.47**
- `entry_side_agreement`: **100.00%**

### Exit reason distribution (expected vs actual)
Baseline and candidate both observed:
- expected: `{stop_loss: 10, stop_and_target: 1, time_stop: 1, take_profit: 1, vwap_mean_reversion: 1}`
- actual: `{vwap_mean_reversion: 4, stop_loss: 6, max_hold_time: 2}`

## Repro commands

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

# baseline
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_postupdate.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T03:35:47Z --end 2026-03-01T03:35:47Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":14,"rsi_oversold":34,"rsi_overbought":66,"deviation_pct":0.9,"ema_filter_period":100,"max_atr_pct":3.0,"vwap_tolerance":0.0018,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_b_before.json \
  --out-md /tmp/tuning_set_b_before.md

# candidate
PYTHONPATH=src python scripts/parity_harness.py \
  --config /tmp/rmr_xrp_tuning_set_b_candidate.yaml \
  --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
  --mode paper --fixed-epoch \
  --start 2026-02-28T03:35:47Z --end 2026-03-01T03:35:47Z \
  --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
  --runtime-strategy-params-json '{"vwap_lookback":20,"rsi_period":10,"rsi_oversold":30,"rsi_overbought":70,"deviation_pct":0.9,"ema_filter_period":100,"max_atr_pct":3.0,"vwap_tolerance":0.0018,"max_hold_candles":15,"stop_loss_enabled":true,"stop_loss_multiplier":1.5}' \
  --enforce-strategy-match \
  --out-json /tmp/tuning_set_b_after.json \
  --out-md /tmp/tuning_set_b_after.md
```

## Gate implication

Candidate improves entry-lane count alignment and match-rate, but **still fails** acceptance thresholds (`signal_delta<=5`, `avg_price_delta_bps<=15`).

Expected gate after applying this candidate: **BLOCK remains** until further iteration.
