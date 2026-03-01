from pathlib import Path


def test_tuning_set_i_sql_targets_expected_strategy_and_params():
    sql_path = Path(__file__).resolve().parents[1] / "scripts" / "tuning_set_i_rmr_xrp_5m.sql"
    sql = sql_path.read_text(encoding="utf-8")

    assert "236d3378-1be5-4264-ac97-79c9d0dbaf12" in sql
    assert "name = 'range_mean_reversion'" in sql
    assert "mode = 'paper'" in sql

    # Single-variable Set I timing delta
    assert "'ema_filter_period', 8" in sql

    # Entry controls pinned
    assert "'vwap_lookback', 20" in sql
    assert "'rsi_period', 10" in sql
    assert "'rsi_oversold', 30" in sql
    assert "'rsi_overbought', 70" in sql
    assert "'deviation_pct', 0.9" in sql

    # Exit/risk controls unchanged
    assert "'max_hold_candles', 15" in sql
    assert "'stop_loss_enabled', true" in sql
    assert "'stop_loss_multiplier', 1.5" in sql
