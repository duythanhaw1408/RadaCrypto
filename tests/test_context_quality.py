import json
from pathlib import Path
from datetime import datetime, timezone
from cfte.cli.reliability import LiveRuntimeArtifact, persist_live_runtime_artifact

def test_live_runtime_artifact_persistence(tmp_path):
    artifact_path = tmp_path / "test_artifact.json"
    artifact = LiveRuntimeArtifact(
        symbol="BTCUSDT",
        status="completed",
        started_at=datetime.now(tz=timezone.utc).isoformat(),
        finished_at=datetime.now(tz=timezone.utc).isoformat(),
        processed_events=100,
        event_counts={"aggTrade": 100},
        reconnect_count=1,
        message_count=100,
        idle_timeout_seconds=45.0,
        heartbeat_interval=250,
        stale_gap_seconds=0.5,
        last_error=None,
        last_trade_ts=1700000000000,
        futures_ws_latency_ms=120,
        futures_is_stale=False,
        collector_health={"binance_spot": {"connected": True, "is_stale": False}},
        context_health={"futures_context_fresh": True, "venue_confirmation_state": "CONFIRMED"},
        latest_tpfm={"matrix_alias_vi": "Thuận pha mua", "tradability_grade": "A"},
        first_m5_seen_at="2026-03-20T05:20:49+00:00",
        latest_transition={"alias_vi": "Tiếp diễn mua", "transition_family": "CONTINUATION"},
        latest_flow_grade="A",
        degraded_flags=[],
    )
    
    persist_live_runtime_artifact(artifact_path, artifact)
    
    assert artifact_path.exists()
    with open(artifact_path, "r") as f:
        data = json.load(f)
    
    assert data["symbol"] == "BTCUSDT"
    assert data["futures_ws_latency_ms"] == 120
    assert data["futures_is_stale"] is False
    assert data["processed_events"] == 100
    assert data["collector_health"]["binance_spot"]["connected"] is True
    assert data["context_health"]["venue_confirmation_state"] == "CONFIRMED"
    assert data["latest_tpfm"]["tradability_grade"] == "A"
    assert data["first_m5_seen_at"] == "2026-03-20T05:20:49+00:00"
    assert data["latest_transition"]["alias_vi"] == "Tiếp diễn mua"
    assert data["latest_flow_grade"] == "A"
