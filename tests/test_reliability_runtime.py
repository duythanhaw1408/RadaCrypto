import json
from pathlib import Path

from cfte.cli.reliability import _check_artifacts, load_json_artifact


def test_load_json_artifact_returns_none_for_invalid_json(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not-json", encoding="utf-8")

    assert load_json_artifact(path) is None


def test_check_artifacts_warns_on_degraded_live_runtime(tmp_path):
    runtime_path = tmp_path / "live_runtime.json"
    runtime_path.write_text(
        json.dumps(
            {
                "symbol": "BTCUSDT",
                "status": "watchdog_timeout",
                "started_at": "2026-03-20T00:00:00+00:00",
                "finished_at": "2026-03-20T00:05:00+00:00",
                "processed_events": 100,
                "event_counts": {"aggTrade": 100},
                "reconnect_count": 1,
                "message_count": 100,
                "idle_timeout_seconds": 45.0,
                "heartbeat_interval": 250,
                "stale_gap_seconds": 31.2,
                "last_error": "Watchdog timeout",
                "last_trade_ts": 1700000000000,
                "futures_ws_latency_ms": 25000,
                "futures_is_stale": True,
                "collector_health": {
                    "binance_futures": {
                        "connected": True,
                        "is_stale": True,
                    }
                },
                "context_health": {
                    "futures_context_fresh": False,
                    "venue_confirmation_state": "UNCONFIRMED",
                    "active_venues": 1,
                },
                "first_m5_seen_at": "2026-03-20T00:02:00+00:00",
                "latest_transition": {
                    "alias_vi": "Tiếp diễn mua",
                    "transition_family": "CONTINUATION",
                },
                "latest_flow_grade": "B",
                "latest_tpfm": {
                    "matrix_alias_vi": "Thuận pha mua",
                    "tradability_grade": "B",
                    "blind_spot_flags": ["NO_FUTURES_DELTA"],
                },
                "degraded_flags": ["futures_context_stale", "multi_venue_unconfirmed"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    checks = _check_artifacts({"live_runtime": runtime_path})
    by_key = {check.key: check for check in checks}

    assert by_key["live_runtime"].status == "ok"
    assert by_key["live_runtime_status"].status == "warn"
    assert by_key["live_runtime_degraded_flags"].status == "warn"
    assert by_key["live_runtime_futures_context"].status == "warn"
    assert by_key["live_runtime_venue_context"].status == "warn"
    assert by_key["live_runtime_first_m5"].status == "ok"
    assert by_key["live_runtime_latest_matrix"].status == "ok"
    assert "futures_context_stale" in (by_key["live_runtime_degraded_flags"].detail or "")
    assert by_key["live_runtime_last_transition"].status == "ok"


def test_check_artifacts_warns_when_live_runtime_has_no_matrix(tmp_path):
    runtime_path = tmp_path / "live_runtime.json"
    runtime_path.write_text(
        json.dumps(
            {
                "symbol": "BTCUSDT",
                "status": "completed",
                "started_at": "2026-03-20T00:00:00+00:00",
                "finished_at": "2026-03-20T00:03:00+00:00",
                "processed_events": 1000,
                "event_counts": {"aggTrade": 1000},
                "reconnect_count": 1,
                "message_count": 1000,
                "idle_timeout_seconds": 45.0,
                "heartbeat_interval": 250,
                "stale_gap_seconds": 0.3,
                "last_error": None,
                "last_trade_ts": 1700000000000,
                "collector_health": {},
                "context_health": {},
                "first_m5_seen_at": None,
                "latest_tpfm": {},
                "degraded_flags": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    checks = _check_artifacts({"live_runtime": runtime_path})
    by_key = {check.key: check for check in checks}

    assert by_key["live_runtime"].status == "ok"
    assert by_key["live_runtime_latest_matrix"].status == "warn"
    assert by_key["live_runtime_first_m5"].status == "warn"
