from pathlib import Path


def test_tuning_set_b_sql_targets_expected_strategy_and_params():
    sql_path = Path(__file__).resolve().parents[1] / "scripts" / "tuning_set_b_rmr_xrp_5m.sql"
    sql = sql_path.read_text(encoding="utf-8")

    assert "236d3378-1be5-4264-ac97-79c9d0dbaf12" in sql
    assert "name = 'range_mean_reversion'" in sql
    assert "mode = 'paper'" in sql

    # Core candidate deltas
    assert "'rsi_period', 10" in sql
    assert "'rsi_oversold', 30" in sql
    assert "'rsi_overbought', 70" in sql

    # Exit/risk fields remain explicitly pinned for deterministic hashing
    assert "'vwap_tolerance', 0.0018" in sql
    assert "'max_hold_candles', 15" in sql
    assert "'stop_loss_multiplier', 1.5" in sql
