"""Tests for strategy runtime fingerprint helpers."""

from __future__ import annotations

from varon_fi import Signal

from signal_service.strategy.engine import StrategyEngine


def test_params_hash_is_order_insensitive():
    engine = StrategyEngine("postgresql://localhost/varon_fi")

    hash_a = engine._params_hash({"b": 2, "a": 1})
    hash_b = engine._params_hash({"a": 1, "b": 2})

    assert hash_a == hash_b


def test_attach_strategy_fingerprint_meta_enriches_signal_meta():
    engine = StrategyEngine("postgresql://localhost/varon_fi")
    engine._strategy_fingerprints["s1"] = {
        "strategy_name": "range_mean_reversion",
        "strategy_version": "1.1.0",
        "module_path": "/tmp/rmr.py",
        "module_sha256": "abc123",
        "git_commit": "deadbeef",
        "params_hash": "cafebabe",
    }

    signal = Signal(side="long", price=1.23, confidence=0.9, meta={"mode": "paper"})
    engine._attach_strategy_fingerprint_meta(signal, "s1")

    assert signal.meta["mode"] == "paper"
    assert signal.meta["strategy_runtime_name"] == "range_mean_reversion"
    assert signal.meta["strategy_runtime_version"] == "1.1.0"
    assert signal.meta["strategy_artifact_path"] == "/tmp/rmr.py"
    assert signal.meta["strategy_artifact_hash"] == "abc123"
    assert signal.meta["strategy_artifact_git_commit"] == "deadbeef"
    assert signal.meta["strategy_params_hash"] == "cafebabe"


def test_build_strategy_fingerprint_includes_module_hash_and_params_hash():
    engine = StrategyEngine("postgresql://localhost/varon_fi")

    class DummyStrategy:
        name = "dummy"
        version = "1.0.0"
        params = {"alpha": 1, "beta": True}

    fp = engine._build_strategy_fingerprint(DummyStrategy())

    assert fp["strategy_name"] == "dummy"
    assert fp["strategy_version"] == "1.0.0"
    assert fp["module_path"].endswith("test_strategy_fingerprint.py")
    assert len(fp["module_sha256"]) == 64
    assert fp["params_hash"] == engine._params_hash({"alpha": 1, "beta": True})
