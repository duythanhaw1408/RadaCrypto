from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


def _load_run_cycle_module():
    module_path = Path("scripts/run_cycle.py")
    spec = importlib.util.spec_from_file_location("run_cycle_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_live_args_requires_first_m5_by_default():
    module = _load_run_cycle_module()
    args = argparse.Namespace(
        symbol="BTCUSDT",
        max_events=1500,
        min_runtime_seconds=330.0,
        allow_missing_m5=False,
    )

    live_args = module._build_live_args(args)

    assert "--max-events" in live_args
    assert "--min-runtime-seconds" in live_args
    assert "--run-until-first-m5" in live_args


def test_runtime_has_m5_detects_matrix_cell(tmp_path):
    module = _load_run_cycle_module()
    runtime_path = tmp_path / "live_runtime.json"
    runtime_path.write_text(
        json.dumps({"latest_tpfm": {"matrix_cell": "POS_INIT__POS_INV"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    assert module._runtime_has_m5(runtime_path) is True


def test_runtime_has_m5_accepts_first_m5_contract(tmp_path):
    module = _load_run_cycle_module()
    runtime_path = tmp_path / "live_runtime.json"
    runtime_path.write_text(
        json.dumps({"first_m5_seen_at": "2026-03-20T05:20:49+00:00", "latest_tpfm": {}}, ensure_ascii=False),
        encoding="utf-8",
    )

    assert module._runtime_has_m5(runtime_path) is True


def test_runtime_has_m5_rejects_empty_latest_matrix(tmp_path):
    module = _load_run_cycle_module()
    runtime_path = tmp_path / "live_runtime.json"
    runtime_path.write_text(
        json.dumps({"latest_tpfm": {}}, ensure_ascii=False),
        encoding="utf-8",
    )

    assert module._runtime_has_m5(runtime_path) is False
