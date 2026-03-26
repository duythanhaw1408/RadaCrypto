import json
from pathlib import Path
import pytest
from cfte.live.engine import LiveThesisLoop
from cfte.cli.main import _sync_scan_dashboard_artifacts

@pytest.mark.asyncio
async def test_run_id_alignment_live(tmp_path):
    # Setup directories
    data_dir = tmp_path / "data"
    review_dir = data_dir / "review"
    review_dir.mkdir(parents=True)
    docs_data_dir = tmp_path / "docs" / "data"
    docs_data_dir.mkdir(parents=True)
    
    # Mock engine
    # Note: We need to override the dashboard sync to use our tmp_path
    class TestEngine(LiveThesisLoop):
        async def _sync_to_dashboard(self):
            # Override to use tmp_path
            dashboard_data_dir = docs_data_dir.absolute()
            await self.store.migrate_schema() # Important: test mock must also bootstrap
            await self._sync_live_summary(dashboard_data_dir, has_data=False)
            await self._sync_actions_status(dashboard_data_dir, has_data=False)
            self._export_flow_artifacts(dashboard_data_dir)
            self._publish_run_bundle(dashboard_data_dir, self._runtime_run_id)

    engine = TestEngine(symbol="BTCUSDT", db_path=tmp_path / "test.db")
    run_id = engine._runtime_run_id
    
    # Manually trigger sync
    await engine._sync_to_dashboard()
    
    # Verify actions_status
    status_path = docs_data_dir / "actions_status.json"
    assert status_path.exists()
    status_data = json.loads(status_path.read_text())
    assert status_data["artifact_run_id"] == run_id
    
    # Verify flow_stack_live (even if empty, should have the correct run_id in the structure)
    stack_path = docs_data_dir / "flow_stack_live.json"
    if stack_path.exists():
        stack_json = json.loads(stack_path.read_text())
        # Access 'items' key for V2 contract
        items = stack_json.get("items", [])
        for row in items:
            assert row["run_id"] == run_id

    manifest_path = docs_data_dir / "current_live.json"
    assert not manifest_path.exists()


@pytest.mark.asyncio
async def test_export_flow_artifacts_preserves_previous_live_bundle_when_run_is_empty(tmp_path):
    docs_data_dir = tmp_path / "docs" / "data"
    docs_data_dir.mkdir(parents=True)

    previous_run = "stable-run"
    previous_payload = {
        "artifact_contract": {
            "schema_version": "v1",
            "mode": "live",
            "run_id": previous_run,
            "symbol": "BTCUSDT",
            "generated_at_ts": 1,
            "artifact_name": "flow_frames",
            "count": 1,
        },
        "items": [{"run_id": previous_run, "frame": "M5"}],
    }
    for name in ("flow_frames_live.json", "flow_timeline_live.json", "flow_stack_live.json"):
        (docs_data_dir / name).write_text(json.dumps(previous_payload), encoding="utf-8")

    engine = LiveThesisLoop(symbol="BTCUSDT", db_path=tmp_path / "empty.db")
    await engine.store.migrate_schema()

    exported_count = engine._export_flow_artifacts(docs_data_dir)
    assert exported_count == 0

    for name in ("flow_frames_live.json", "flow_timeline_live.json", "flow_stack_live.json"):
        payload = json.loads((docs_data_dir / name).read_text())
        assert payload["artifact_contract"]["run_id"] == previous_run
        assert payload["artifact_contract"]["count"] == 1


def test_publish_run_bundle_keeps_existing_manifest_when_flow_bundle_is_incomplete(tmp_path):
    docs_data_dir = tmp_path / "docs" / "data"
    docs_data_dir.mkdir(parents=True)

    old_run = "stable-run"
    new_run = "new-run"

    (docs_data_dir / "current_live.json").write_text(
        json.dumps(
            {
                "schema_version": "v1",
                "mode": "live",
                "run_id": old_run,
                "paths": {"frames": f"runs/{old_run}/flow_frames_live.json"},
            }
        ),
        encoding="utf-8",
    )
    (docs_data_dir / "actions_status.json").write_text(
        json.dumps({"artifact_run_id": new_run, "data_mode": "live"}),
        encoding="utf-8",
    )
    (docs_data_dir / "summary_btcusdt_live.json").write_text(
        json.dumps(
            {
                "artifact_contract": {
                    "run_id": new_run,
                    "mode": "live",
                }
            }
        ),
        encoding="utf-8",
    )
    for name in ("flow_frames_live.json", "flow_timeline_live.json", "flow_stack_live.json"):
        (docs_data_dir / name).write_text(
            json.dumps(
                {
                    "artifact_contract": {
                        "run_id": old_run,
                        "mode": "live",
                        "count": 0,
                    },
                    "items": [],
                }
            ),
            encoding="utf-8",
        )

    engine = LiveThesisLoop(symbol="BTCUSDT", db_path=tmp_path / "empty.db")
    engine._publish_run_bundle(docs_data_dir, new_run)

    manifest = json.loads((docs_data_dir / "current_live.json").read_text())
    assert manifest["run_id"] == old_run
    assert not (docs_data_dir / "runs" / new_run / "flow_frames_live.json").exists()


def test_scan_manifest_uses_scan_suffixed_mtf_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    review_dir = tmp_path / "data" / "review"
    thesis_dir = tmp_path / "data" / "thesis"
    review_dir.mkdir(parents=True)
    thesis_dir.mkdir(parents=True)

    (review_dir / "tpfm_m5_scan.json").write_text(json.dumps([{"run_id": "scan-run"}]), encoding="utf-8")
    (review_dir / "tpfm_m30.json").write_text(json.dumps([{"run_id": "scan-run"}]), encoding="utf-8")
    (review_dir / "tpfm_4h.json").write_text(json.dumps([{"run_id": "scan-run"}]), encoding="utf-8")
    (review_dir / "daily_summary.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (review_dir / "health_status.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    (thesis_dir / "thesis_log.jsonl").write_text(
        json.dumps({"run_id": "scan-run", "event_type": "stage_transition"}) + "\n",
        encoding="utf-8",
    )

    summary_out = tmp_path / "summary_btcusdt_scan.json"
    summary_out.write_text(
        json.dumps(
            {
                "artifact_contract": {
                    "schema_version": "v2",
                    "mode": "scan",
                    "run_id": "scan-run",
                    "generated_at": "2026-03-26T00:00:00+00:00",
                    "window_end_ts": 1,
                },
                "top_signals": [],
                "latest_tpfm": {},
            }
        ),
        encoding="utf-8",
    )

    _sync_scan_dashboard_artifacts(summary_out)

    docs_data_dir = tmp_path / "docs" / "data"
    manifest = json.loads((docs_data_dir / "current_scan.json").read_text())
    assert manifest["paths"]["m30"] == "runs/scan-run/tpfm_m30_scan.json"
    assert manifest["paths"]["h4"] == "runs/scan-run/tpfm_4h_scan.json"
    assert (docs_data_dir / manifest["paths"]["m30"]).exists()
    assert (docs_data_dir / manifest["paths"]["h4"]).exists()
