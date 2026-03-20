import json
import os
from pathlib import Path
from datetime import datetime, timezone
import pytest

from cfte.cli.reliability import (
    LiveRuntimeArtifact,
    acquire_live_runtime_lease,
    persist_live_runtime_artifact,
    release_live_runtime_lease,
)

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


def test_live_runtime_lease_blocks_competing_writer(tmp_path):
    artifact_path = tmp_path / "live_runtime.json"
    lock_path = artifact_path.with_name(f"{artifact_path.name}.lock")
    lock_path.write_text(
        json.dumps(
            {
                "artifact_path": str(artifact_path),
                "lock_path": str(lock_path),
                "run_id": "other-run",
                "pid": os.getpid(),
                "host": "audit-host",
                "acquired_at": "2026-03-20T00:00:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Runtime artifact"):
        acquire_live_runtime_lease(artifact_path, run_id="current-run")


def test_live_runtime_lease_roundtrip_and_guard(tmp_path):
    artifact_path = tmp_path / "live_runtime.json"
    lease = acquire_live_runtime_lease(artifact_path, run_id="run-1")
    artifact = LiveRuntimeArtifact(
        symbol="BTCUSDT",
        status="completed",
        started_at=datetime.now(tz=timezone.utc).isoformat(),
        finished_at=datetime.now(tz=timezone.utc).isoformat(),
        processed_events=10,
        event_counts={"aggTrade": 10},
        reconnect_count=0,
        message_count=10,
        idle_timeout_seconds=45.0,
        heartbeat_interval=250,
        stale_gap_seconds=0.1,
        last_error=None,
        last_trade_ts=1700000000000,
        pid=lease.pid,
        run_id=lease.run_id,
        owner_host=lease.host,
        lock_path=str(lease.lock_path),
        lock_acquired_at=lease.acquired_at,
    )

    persist_live_runtime_artifact(artifact_path, artifact, lease=lease)
    assert artifact_path.exists()
    assert lease.lock_path.exists()

    release_live_runtime_lease(lease)
    assert not lease.lock_path.exists()

    with pytest.raises(RuntimeError, match="Runtime lock bị mất"):
        persist_live_runtime_artifact(artifact_path, artifact, lease=lease)
