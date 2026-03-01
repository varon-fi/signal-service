# Tuning Set C — XRP 5m (`range_mean_reversion`)

Issue: https://github.com/varon-fi/signal-service/issues/39  
Downstream gate: https://github.com/varon-fi/orders-service/issues/95

## Scope

Entry timing/price alignment only:
- keep RSI params fixed from Set B (`rsi_period=10`, `rsi_oversold=30`, `rsi_overbought=70`)
- keep all exit controls unchanged (`max_hold_candles=15`, `stop_loss_enabled=true`, `stop_loss_multiplier=1.5`)
- adjust only `vwap_tolerance`

## Candidate selected for implementation PR

```json
{
  "vwap_lookback": 20,
  "rsi_period": 10,
  "rsi_oversold": 30,
  "rsi_overbought": 70,
  "deviation_pct": 0.9,
  "ema_filter_period": 100,
  "max_atr_pct": 3.0,
  "vwap_tolerance": 0.0015,
  "max_hold_candles": 15,
  "stop_loss_enabled": true,
  "stop_loss_multiplier": 1.5
}
```

## Fixed-epoch parity evidence (vs accepted baseline)

Epoch (locked): `2026-02-28T08:17:13Z` → `2026-03-01T08:17:13Z`

### Baseline accepted on #95
- `entry_match_rate`: **27.27%** (`6/22`)
- `entry_signal_delta_pct`: **63.64%**
- `entry_avg_price_delta_bps`: **12.74**
- `entry_side_agreement`: **100.00%**
- Gate: **BLOCK**

### Set C sweep (`vwap_tolerance` only)
| vwap_tolerance | entry_match_rate | entry_signal_delta_pct | entry_avg_price_delta_bps | entry_side_agreement |
|---|---:|---:|---:|---:|
| 0.0012 | 27.27% | 63.64% | 12.74 | 100.00% |
| 0.0015 | 27.27% | 63.64% | 12.74 | 100.00% |
| 0.0018 | 27.27% | 63.64% | 12.74 | 100.00% |

Observation: within this sweep band, entry metrics are unchanged on the fixed epoch.

## Repro commands

```bash
cd /home/varon/.openclaw/agents/loretta/workspace/backtest

for VT in 0.0012 0.0015 0.0018; do
  CFG="/tmp/rmr_xrp_tuning_set_c_${VT/./p}.yaml"
  cat > "$CFG" <<EOF
 db:
   url: "postgresql://postgres:1UkEV99XLipoK8T8z4qJ@localhost:5432/varon_fi"
 session:
   timezone: "UTC"
   start: "00:00"
   end: "23:59"
 data:
   timeframe: "5m"
   exchange_id: 1
   source: "auto"
   symbols: ["XRP"]
 fees:
   maker_bps: 2.0
   taker_bps: 5.0
   slippage_bps: 2.0
 risk:
   initial_equity: 10000.0
   risk_per_trade: 0.005
   max_leverage: 2.0
   max_drawdown: 0.20
 strategy:
   name: "range_mean_reversion"
   params:
     vwap_lookback: 20
     rsi_period: 10
     rsi_oversold: 30
     rsi_overbought: 70
     deviation_pct: 0.9
     ema_filter_period: 100
     max_atr_pct: 3.0
     vwap_tolerance: ${VT}
     max_hold_candles: 15
     stop_loss_enabled: true
     stop_loss_multiplier: 1.5
EOF

  PYTHONPATH=src python scripts/parity_harness.py \
    --config "$CFG" \
    --strategy-id 236d3378-1be5-4264-ac97-79c9d0dbaf12 \
    --mode paper --fixed-epoch \
    --start 2026-02-28T08:17:13Z --end 2026-03-01T08:17:13Z \
    --runtime-strategy-name range_mean_reversion --runtime-timeframe 5m \
    --runtime-strategy-params-json "{\"vwap_lookback\":20,\"rsi_period\":10,\"rsi_oversold\":30,\"rsi_overbought\":70,\"deviation_pct\":0.9,\"ema_filter_period\":100,\"max_atr_pct\":3.0,\"vwap_tolerance\":${VT},\"max_hold_candles\":15,\"stop_loss_enabled\":true,\"stop_loss_multiplier\":1.5}" \
    --enforce-strategy-match \
    --out-json "/tmp/tuning_set_c_vwap_${VT/./p}.json" \
    --out-md "/tmp/tuning_set_c_vwap_${VT/./p}.md"
done
```

## Gate implication

Set C (vwap-only) does not yet clear the entry delta threshold on this fixed epoch.  
Expected post-merge gate remains **BLOCK** until a subsequent tuning axis reduces `entry_signal_delta_pct`.
