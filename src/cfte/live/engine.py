from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import asdict, dataclass, field
import socket
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import shutil
import json
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

from cfte.books.binance_depth import BinanceDepthReconciler
from cfte.collectors.binance_public import BinancePublicCollector, build_public_streams, try_fetch_depth_snapshot
from cfte.collectors.binance_futures import BinanceFuturesCollector
from cfte.collectors.health import CollectorHealthSnapshot, build_error_surface
from cfte.features.tape import (
    DEFAULT_MAX_WINDOW_TRADES,
    DEFAULT_TRADE_WINDOW_SECONDS,
    build_tape_snapshot,
    slice_trade_window,
)
from cfte.normalizers.binance import (
    normalize_agg_trade,
    normalize_book_ticker,
    normalize_depth_diff,
    normalize_trade,
)
from cfte.cli.reliability import (
    LiveRuntimeArtifact,
    LiveRuntimeLease,
    acquire_live_runtime_lease,
    persist_live_runtime_artifact,
    release_live_runtime_lease,
)
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.storage.thesis_log import ThesisLogWriter
from cfte.live.outcome_monitor import OutcomeMonitor
from cfte.live.outcome_realism import OutcomeRealismEngine
from cfte.thesis.lifecycle import ACTIVE_STAGES
from cfte.thesis.engines import evaluate_setups
from cfte.thesis.veto import VetoEngine
from cfte.thesis.state import ThesisLifecycleRecord, apply_signal_update
from cfte.tpfm.engine import TPFMStateEngine
from cfte.tpfm.cards import render_tpfm_m5_card
from cfte.tpfm.ai_explainer import TPFMAIExplainer
from cfte.tpfm.models import TPFMSnapshot
from cfte.models.events import NormalizedTrade, TapeSnapshot

# Phase 4 Imports
from cfte.collectors.bybit_public import BybitPublicCollector, build_public_topics, try_fetch_depth_snapshot as try_fetch_bybit_depth
from cfte.collectors.okx_public import OkxPublicCollector, build_public_args, try_fetch_depth_snapshot as try_fetch_okx_depth
from cfte.normalizers.bybit import normalize_public_trade as normalize_bybit_trade
from cfte.normalizers.okx import normalize_trade as normalize_okx_trade
from cfte.features.venue_compare import compare_trade_flows, build_venue_confirmation_context


@dataclass(slots=True)
class LiveEngineHealth:
    venue: str = "binance"
    connected: bool = False
    message_count: int = 0
    reconnect_count: int = -1  # Starts at -1 because first connect increments it
    last_error: str | None = None


class LiveThesisLoop:
    def __init__(
        self,
        symbol: str,
        db_path: Path,
        use_agg_trade: bool = True,
        horizons: list[str] | None = None,
        thesis_log_path: Path | None = None,
        watchdog_idle_seconds: float = 45.0,
        heartbeat_interval: int = 250,
        runtime_report_path: Path | None = None,
        max_retries: int = 5,
        trade_window_seconds: float = DEFAULT_TRADE_WINDOW_SECONDS,
        max_window_trades: int = DEFAULT_MAX_WINDOW_TRADES,
        min_runtime: int = 0,
        run_until_first_m5: bool = False,
        mode: str = "live",
    ) -> None:
        self.symbol = symbol.upper()
        self.venue = "binance"  # Default venue for live loop
        self.instrument_key = f"BINANCE:{self.symbol}:SPOT"
        self.db_path = db_path
        self.use_agg_trade = use_agg_trade
        self.horizons = horizons or ["1h", "4h", "24h"]
        self.ux = {}  # Will be set by run-live
        self.store = ThesisSQLiteStore(db_path)
        self.thesis_log = ThesisLogWriter(thesis_log_path) if thesis_log_path is not None else None
        self.watchdog_idle_seconds = watchdog_idle_seconds
        self.heartbeat_interval = max(1, heartbeat_interval)
        self.runtime_report_path = runtime_report_path
        self._runtime_run_id = uuid4().hex
        self.mode = mode
        self._runtime_lease: LiveRuntimeLease | None = None
        self.max_retries = max(1, max_retries)
        self.trade_window_seconds = max(1.0, float(trade_window_seconds))
        self.max_window_trades = max(1, int(max_window_trades))
        self._venue_window_ms = max(10_000, int(self.trade_window_seconds * 1000))
        self.health = LiveEngineHealth(venue="binance")
        self.thesis_state: dict[str, ThesisLifecycleRecord] = {}
        self._last_alert_score: dict[str, float] = {}
        self._depth = BinanceDepthReconciler(instrument_key=self.instrument_key)
        self._trades: list[NormalizedTrade] = []
        self._previous_feature_book = None
        self._stop_event = asyncio.Event()
        self._last_message_monotonic: float | None = None
        self._last_trade_ts: int | None = None
        self._collector_health: dict[str, CollectorHealthSnapshot] = {}
        self._context_health: dict[str, str | float | bool | int | list[str] | None] = {}
        self._latest_tpfm_summary: dict[str, str | float | bool | int | list[str] | None] = {}
        self._dashboard_m5_history = deque(maxlen=100)
        self._last_ai_brief_ts: float = 0 # Track last Gemini call time
        self._prev_m5_winning_signal: dict = {}  # Previous M5 winning signal for delta comparison
        self._m5_signal_history: list[dict] = []  # Consolidated M5 signal history (1 per window)
        
        # HTF State Caches (Patch 1: Parent tracking for Flow Stack)
        self._latest_m30_id = ""
        self._latest_h1_id = ""
        self._latest_h4_id = ""
        self._latest_h12_id = ""
        self._latest_d1_id = ""
        self._latest_m30_end_ts = 0
        self._latest_h1_end_ts = 0
        self._latest_h4_end_ts = 0
        self._latest_h12_end_ts = 0
        self._latest_d1_end_ts = 0
        self._latest_frame_meta = {} # Cache for rich frame metadata (alias, bias, grade)
        
        # Load existing history if available
        try:
            m5_path = Path("data/review/tpfm_m5.json")
            if m5_path.exists():
                old_snapshots = json.loads(m5_path.read_text(encoding="utf-8"))
                if isinstance(old_snapshots, list):
                    # Fill the deque with loaded snapshots (as dicts or objects depending on use)
                    # Line 908 indicates it expects objects to call asdict() on them.
                    # So we should reconstruct TPFMSnapshot objects.
                    for s_dict in old_snapshots[-100:]:
                        try:
                            # Reconstruct object, but note TPFMSnapshot has many fields.
                            # We can just use the dict if we adjust the save logic, 
                            # but line 908: [asdict(s) for s in self._dashboard_m5_history]
                            # requires 's' to be a dataclass.
                            # Let's use a simpler approach: load them as TPFMSnapshot objects.
                            # Since TPFMSnapshot is a large dataclass, we can use 
                            # TPFMSnapshot(**s_dict) if the keys match exactly.
                            # However, some fields like transition_event might be nested. 
                            # For dashboard chart, the basic fields are mostly enough.
                            
                            # Pre-filter s_dict to only include fields TPFMSnapshot expects
                            from dataclasses import fields
                            valid_fields = {f.name for f in fields(TPFMSnapshot)}
                            filtered_s = {k: v for k, v in s_dict.items() if k in valid_fields}
                            self._dashboard_m5_history.append(TPFMSnapshot(**filtered_s))
                        except Exception:
                            continue
                print(f"📊 [INIT] Loaded {len(self._dashboard_m5_history)} snapshots from history.")
        except Exception as e:
            print(f"⚠️ [INIT] Failed to load M5 history: {e}")
        
        # TPFM State
        self.tpfm = TPFMStateEngine(symbol=self.symbol)
        self._tpfm_window_start_ts: int | None = None
        self._tpfm_trades: list[NormalizedTrade] = []
        self._tpfm_snapshots: list[TapeSnapshot] = []
        
        # Phase 20-E: MTF Roll Buffers
        self._tpfm_m5_roll_buffer: deque[TPFMSnapshot] = deque(maxlen=288) # 24h
        self._tpfm_h1_roll_buffer: deque[dict] = deque(maxlen=24)
        self._tpfm_h4_roll_buffer: deque[dict] = deque(maxlen=6)
        self._tpfm_h12_roll_buffer: deque[dict] = deque(maxlen=2)
        
        # Phase 20-H: Latest Frame State Trackers
        self._latest_frame_ids: dict[str, str] = {} # frame -> state_id
        self._last_exported_flow_run_id: str | None = None
        self._tpfm_m5_buffer: list[TPFMSnapshot] = []
        self._tpfm_m30_buffer: list[TPFM30mRegime] = []
        self._latest_tpfm_snapshot: TPFMSnapshot | None = None
        self._first_m5_seen_at: str | None = None
        self._last_transition_alias_vi: str | None = None
        self._last_transition_summary: dict[str, str | float | int | bool | None] = {}
        
        # Venue Tracking for Confirmation (Phase 4)
        self._venue_trades: dict[str, deque[NormalizedTrade]] = {
            "binance": deque(maxlen=200),
            "bybit": deque(maxlen=100),
            "okx": deque(maxlen=100),
        }
        
        self.ai_explainer = TPFMAIExplainer()
        self.futures_collector = BinanceFuturesCollector(symbol=self.symbol)
        self.veto_engine = VetoEngine()
        self.outcome_realism = OutcomeRealismEngine(self.store)

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict | list) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)

    @staticmethod
    def _read_json_file(path: Path) -> dict | list | None:
        try:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _filter_records_for_run(self, records: list[dict], run_id: str) -> list[dict]:
        if not records:
            return []
        has_run_key = any(isinstance(item, dict) and "run_id" in item for item in records)
        if not has_run_key:
            return records
        return [item for item in records if isinstance(item, dict) and item.get("run_id") == run_id]

    def _run_bundle_dir(self, dashboard_data_dir: Path, run_id: str) -> Path:
        return (dashboard_data_dir / "runs" / run_id).absolute()

    def _build_dashboard_manifest(self, run_id: str) -> dict:
        mode = self.mode
        return {
            "schema_version": "v1",
            "mode": mode,
            "run_id": run_id,
            "published_at": datetime.now(tz=timezone.utc).isoformat(),
            "paths": {
                "status": f"runs/{run_id}/actions_status.json",
                "summary": f"runs/{run_id}/summary_btcusdt_{mode}.json",
                "frames": f"runs/{run_id}/flow_frames_{mode}.json",
                "timeline": f"runs/{run_id}/flow_timeline_{mode}.json",
                "stack": f"runs/{run_id}/flow_stack_{mode}.json",
                "logs": f"runs/{run_id}/thesis_log_{mode}.json",
                "realtime": f"runs/{run_id}/realtime_events_{mode}.json",
                "m5": f"runs/{run_id}/tpfm_m5_{mode}.json",
                "m30": f"runs/{run_id}/tpfm_m30_{mode}.json",
                "h4": f"runs/{run_id}/tpfm_4h_{mode}.json",
            },
        }

    def _publish_run_bundle(self, dashboard_data_dir: Path, published_run_id: str) -> None:
        run_dir = self._run_bundle_dir(dashboard_data_dir, published_run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        mode = self.mode
        bundle_ready = True

        status_payload = self._read_json_file((dashboard_data_dir / "actions_status.json").absolute())
        if isinstance(status_payload, dict):
            status_copy = dict(status_payload)
            status_copy["artifact_run_id"] = published_run_id
            self._write_json_atomic((run_dir / "actions_status.json").absolute(), status_copy)

        summary_path = (dashboard_data_dir / f"summary_btcusdt_{mode}.json").absolute()
        summary_payload = self._read_json_file(summary_path)
        if (
            isinstance(summary_payload, dict)
            and summary_payload.get("artifact_contract", {}).get("run_id") == published_run_id
        ):
            self._write_json_atomic((run_dir / f"summary_btcusdt_{mode}.json").absolute(), summary_payload)
        else:
            bundle_ready = False

        for artifact_name in (
            f"flow_frames_{mode}.json",
            f"flow_timeline_{mode}.json",
            f"flow_stack_{mode}.json",
        ):
            artifact_path = (dashboard_data_dir / artifact_name).absolute()
            artifact_payload = self._read_json_file(artifact_path)
            if (
                isinstance(artifact_payload, dict)
                and artifact_payload.get("artifact_contract", {}).get("run_id") == published_run_id
            ):
                self._write_json_atomic((run_dir / artifact_name).absolute(), artifact_payload)
            else:
                bundle_ready = False

        thesis_log_path = (dashboard_data_dir / f"thesis_log_{mode}.json").absolute()
        thesis_log_payload = self._read_json_file(thesis_log_path)
        if isinstance(thesis_log_payload, list):
            self._write_json_atomic(
                (run_dir / f"thesis_log_{mode}.json").absolute(),
                self._filter_records_for_run(thesis_log_payload, published_run_id),
            )
        else:
            self._write_json_atomic((run_dir / f"thesis_log_{mode}.json").absolute(), [])

        realtime_path = (dashboard_data_dir / f"realtime_events_{mode}.json").absolute()
        realtime_payload = self._read_json_file(realtime_path)
        if isinstance(realtime_payload, list):
            self._write_json_atomic(
                (run_dir / f"realtime_events_{mode}.json").absolute(),
                self._filter_records_for_run(realtime_payload, published_run_id),
            )
        else:
            self._write_json_atomic((run_dir / f"realtime_events_{mode}.json").absolute(), [])

        for telemetry_name in (f"tpfm_m5_{mode}.json", f"tpfm_m30_{mode}.json", f"tpfm_4h_{mode}.json"):
            telemetry_path = (dashboard_data_dir / telemetry_name).absolute()
            telemetry_payload = self._read_json_file(telemetry_path)
            if isinstance(telemetry_payload, list):
                self._write_json_atomic(
                    (run_dir / telemetry_name).absolute(),
                    self._filter_records_for_run(telemetry_payload, published_run_id),
                )
            else:
                self._write_json_atomic((run_dir / telemetry_name).absolute(), [])

        if not bundle_ready:
            print(f"ℹ️ [Phase24] Skip current_{mode}.json update until bundle is complete for run {published_run_id}")
            return

        manifest = self._build_dashboard_manifest(published_run_id)
        self._write_json_atomic((dashboard_data_dir / f"current_{mode}.json").absolute(), manifest)

    def _should_alert_signal(
        self,
        *,
        prev_stage: str | None,
        next_stage: str,
        score: float,
        score_delta: float,
    ) -> bool:
        score_delta_threshold = float(self.ux.get("alert_on_score_delta", 10.0))
        score_floor = float(self.ux.get("alert_score_floor", 65.0))
        terminal = next_stage not in ACTIVE_STAGES
        stage_changed = next_stage != prev_stage

        if terminal:
            return stage_changed and self.ux.get("alert_on_stage_change", True)

        if stage_changed and self.ux.get("alert_on_stage_change", True):
            if next_stage != "DETECTED" or score >= score_floor:
                return True

        if next_stage in {"WATCHLIST", "CONFIRMED", "ACTIONABLE"} and score >= score_floor:
            return score_delta >= score_delta_threshold

        return False

    @staticmethod
    def _render_event_summary(thesis_id: str, events: list) -> str:
        if not events:
            return ""
        if len(events) == 1:
            event = events[0]
            return f"📝 [EVENT] {thesis_id[:8]} -> {event.to_stage}: {event.summary_vi}"
        path = " -> ".join(event.to_stage for event in events)
        return f"📝 [EVENT] {thesis_id[:8]} -> {events[-1].to_stage}: Chuyển nhanh qua các mốc {path}."

    def _trim_venue_trade_queues(self, end_ts: int) -> None:
        cutoff = end_ts - self._venue_window_ms
        for queue in self._venue_trades.values():
            while queue and queue[0].venue_ts < cutoff:
                queue.popleft()

    def _sync_binance_futures_venue(self, end_ts: int) -> None:
        queue = self._venue_trades["binance"]
        queue.clear()
        for idx, row in enumerate(self.futures_collector.recent_agg_trade_rows(now_ms=end_ts)[-queue.maxlen:]):
            queue.append(
                NormalizedTrade(
                    event_id=f"binance-futures-{row['ts']}-{idx}",
                    venue="binance",
                    instrument_key=f"BINANCE:{self.symbol}:PERP",
                    price=float(row["p"]),
                    qty=float(row["q"]),
                    quote_qty=float(row["quote_qty"]),
                    taker_side="SELL" if bool(row.get("m", False)) else "BUY",
                    venue_ts=int(row["ts"]),
                )
            )

    def _build_venue_context(self, end_ts: int) -> dict[str, str | float]:
        self._sync_binance_futures_venue(end_ts)
        self._trim_venue_trade_queues(end_ts)
        comparison_trades = [trade for queue in self._venue_trades.values() for trade in queue]
        active_venues = sum(1 for queue in self._venue_trades.values() if queue)
        if active_venues < 2:
            return {"venue_confirmation_state": "UNCONFIRMED", "active_venues": active_venues}
        try:
            result = compare_trade_flows(comparison_trades)
        except ValueError:
            return {"venue_confirmation_state": "UNCONFIRMED", "active_venues": active_venues}
        context = build_venue_confirmation_context(result, primary_venue="binance")
        context["active_venues"] = active_venues
        return context

    @staticmethod
    def _collector_snapshot_to_dict(snapshot: CollectorHealthSnapshot) -> dict[str, object]:
        return asdict(snapshot)

    def _refresh_collector_health(
        self,
        *,
        spot_collector: BinancePublicCollector | None = None,
        bybit_collector: BybitPublicCollector | None = None,
        okx_collector: OkxPublicCollector | None = None,
        now_ms: int | None = None,
    ) -> None:
        if spot_collector is not None:
            self._collector_health["binance_spot"] = spot_collector.health_snapshot()
        self._collector_health["binance_futures"] = self.futures_collector.health_snapshot(now_ms=now_ms)
        if bybit_collector is not None:
            self._collector_health["bybit"] = bybit_collector.health_snapshot()
        if okx_collector is not None:
            self._collector_health["okx"] = okx_collector.health_snapshot()

    def _update_context_health(
        self,
        *,
        futures_context: dict[str, object],
        venue_context: dict[str, object],
        event_ts: int,
    ) -> None:
        self._context_health = {
            "as_of_ts": event_ts,
            "gap_seconds": self._stale_gap_seconds(),
            "futures_context_available": bool(futures_context.get("available", False)),
            "futures_context_fresh": bool(futures_context.get("fresh", False)),
            "futures_delta_available": bool(futures_context.get("futures_delta_available", False)),
            "liquidation_context_available": bool(futures_context.get("liquidation_context_available", False)),
            "liquidation_bias": str(futures_context.get("liquidation_bias", "UNKNOWN")),
            "venue_confirmation_state": str(venue_context.get("venue_confirmation_state", "UNCONFIRMED")),
            "leader_venue": str(venue_context.get("leader_venue", "")),
            "active_venues": int(venue_context.get("active_venues", 0) or 0),
        }

    def _update_latest_tpfm_summary(self, tpfm_snap) -> None:
        self._latest_tpfm_summary = {
            "window_end_ts": int(tpfm_snap.window_end_ts),
            "matrix_cell": str(tpfm_snap.matrix_cell),
            "matrix_alias_vi": str(tpfm_snap.matrix_alias_vi),
            "tradability_grade": str(tpfm_snap.tradability_grade),
            "flow_state_code": str(getattr(tpfm_snap, "flow_state_code", "")),
            "decision_posture": str(getattr(tpfm_snap, "decision_posture", "")),
            "continuation_bias": str(tpfm_snap.continuation_bias),
            "health_state": str(tpfm_snap.health_state),
            "spot_futures_relation": str(tpfm_snap.spot_futures_relation),
            "venue_confirmation_state": str(tpfm_snap.venue_confirmation_state),
            "blind_spot_flags": list(tpfm_snap.blind_spot_flags),
            "risk_flags": list(tpfm_snap.risk_flags[:4]),
            "action_plan_vi": str(tpfm_snap.action_plan_vi),
            "flow_decision_brief": str(tpfm_snap.flow_decision_brief),
        }

    def _runtime_degraded_flags(self) -> list[str]:
        flags: list[str] = []
        gap_seconds = self._stale_gap_seconds()
        if gap_seconds >= max(5.0, self.watchdog_idle_seconds * 0.5):
            flags.append("spot_idle_gap_high")

        for name, snapshot in self._collector_health.items():
            if snapshot.is_stale:
                flags.append(f"{name}_stale")
            elif snapshot.state == "degraded":
                flags.append(f"{name}_degraded")

        if self._context_health:
            if not bool(self._context_health.get("futures_context_fresh", False)):
                flags.append("futures_context_stale")
            if not bool(self._context_health.get("futures_delta_available", False)):
                flags.append("futures_delta_missing")
            if not bool(self._context_health.get("liquidation_context_available", False)):
                flags.append("liquidation_context_missing")
            if (
                str(self._context_health.get("venue_confirmation_state", "UNCONFIRMED")) == "UNCONFIRMED"
                and int(self._context_health.get("active_venues", 0) or 0) < 2
            ):
                flags.append("multi_venue_unconfirmed")

        if self._latest_tpfm_summary and str(self._latest_tpfm_summary.get("health_state", "HEALTHY")) != "HEALTHY":
            flags.append("tpfm_snapshot_degraded")

        return sorted(set(flags))

    async def _dashboard_sync_loop(self):
        """Periodic background task to sync dashboard data."""
        while not self._stop_event.is_set():
            await self._sync_to_dashboard()
            # Wait for 30s or until stop
            for _ in range(30):
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(1)

    async def _sync_to_dashboard(self):
        """
        Syncs latest telemetry to the docs/data folder for the professional UI.
        Uses _live.json suffixes to avoid colliding with scan artifacts.
        Uses absolute paths to avoid working directory ambiguity.
        """
        try:
            # Ensure DB schema is ready
            await self.store.migrate_schema()
            
            # Dashboard target directory
            dashboard_data_dir = Path("docs/data").absolute()
            if not dashboard_data_dir.exists():
                dashboard_data_dir.mkdir(parents=True, exist_ok=True)

            # 1. Sync Thesis Log (.jsonl to .json array)
            def _safe_sync_log():
                src_log = Path("data/thesis/thesis_log.jsonl").absolute()
                if src_log.exists():
                    try:
                        with src_log.open("r", encoding="utf-8") as f:
                            records = [json.loads(line) for line in f if line.strip()]
                        # Only sync last 100 for performance
                        target_log = dashboard_data_dir / f"thesis_log_{self.mode}.json"
                        with target_log.open("w", encoding="utf-8") as f:
                            json.dump(records[-100:], f, ensure_ascii=False)
                    except Exception as e:
                        print(f"⚠️ [Phase20B] Sync Thesis Log failed: {e}")

            _safe_sync_log()

            # 2. Phase 20-B: Export market-timeline-anchored flow artifacts FIRST
            # This returns the count of frames exported
            frames_count = 0
            try:
                frames_count = self._export_flow_artifacts(dashboard_data_dir)
            except Exception as e:
                print(f"⚠️ [Phase20B] Sync flow artifacts failed: {e}")

            has_current_run_flow = (
                frames_count > 0 and self._last_exported_flow_run_id == self._runtime_run_id
            )

            # 3. Generate and Sync TRUE Live Summary
            # We only sync summary if we have some data to show
            try:
                await self._sync_live_summary(dashboard_data_dir, has_data=has_current_run_flow)
            except Exception as e:
                print(f"⚠️ [Phase20B] Sync live summary failed: {e}")

            # 4. Sync Telemetry Files
            mode_suffix = f"_{self.mode}"
            telemetry_map = {
                f"data/review/tpfm_m5.json": f"tpfm_m5{mode_suffix}.json",
                "data/review/daily_summary.json": "daily_summary.json",
                "data/review/health_status.json": "health_status.json",
                f"data/review/tpfm_m30.json": f"tpfm_m30{mode_suffix}.json",
                f"data/review/tpfm_4h.json": f"tpfm_4h{mode_suffix}.json",
            }

            for src_rel, target_name in telemetry_map.items():
                try:
                    src_p = Path(src_rel).absolute()
                    if src_p.exists():
                        shutil.copy2(src_p, dashboard_data_dir / target_name)
                except Exception as e:
                    print(f"⚠️ [Phase20B] Sync {target_name} failed: {e}")

            # 5. Export realtime thesis events
            try:
                await self._sync_realtime_events(dashboard_data_dir)
            except Exception as e:
                print(f"⚠️ [Phase20B] Sync realtime events failed: {e}")

            # 6. Update actions_status.json LAST with a publication gate
            # ONLY update artifact_run_id if we have at least one frame
            try:
                await self._sync_actions_status(dashboard_data_dir, has_data=has_current_run_flow)
            except Exception as e:
                print(f"⚠️ [Phase20B] Sync actions status failed: {e}")

            # 7. Publish an atomic per-run bundle + manifest for dashboard reads
            try:
                status_payload = self._read_json_file((dashboard_data_dir / "actions_status.json").absolute())
                published_run_id = (
                    status_payload.get("artifact_run_id")
                    if isinstance(status_payload, dict)
                    else self._runtime_run_id
                )
                if published_run_id:
                    self._publish_run_bundle(dashboard_data_dir, published_run_id)
            except Exception as e:
                print(f"⚠️ [Phase20B] Publish current manifest failed: {e}")
                
        except Exception as global_e:
            print(f"❌ [Phase20B] Critical failure in _sync_to_dashboard: {global_e}")

    def _select_m5_winning_signal(self) -> dict:
        """Select the single best signal from current thesis state for M5 consolidated view."""
        active_list = list(self.thesis_state.values())
        if not active_list:
            return {}
        # Pick the signal with highest (flow_alignment_score, score) — same sorting as evaluate_setups
        best_record = max(
            active_list,
            key=lambda x: (getattr(x.signal, 'flow_alignment_score', 0), x.signal.score, x.signal.confidence),
        )
        winner = asdict(best_record.signal)
        winner["created_at"] = winner.get("created_at") or datetime.now(tz=timezone.utc).isoformat()
        return winner

    def _build_m5_consolidated_entry(self, winner: dict) -> dict:
        """Build a consolidated M5 entry with delta comparison to previous M5."""
        prev = self._prev_m5_winning_signal
        delta_score = round(winner.get("score", 0) - prev.get("score", 0), 1) if prev else 0
        prev_grade = prev.get("tradability_grade", "") if prev else ""
        curr_grade = winner.get("tradability_grade", "D")
        prev_setup = prev.get("setup", "") if prev else ""
        curr_setup = winner.get("setup", "")
        
        entry = {
            **winner,
            "delta_score": delta_score,
            "prev_grade": prev_grade,
            "grade_changed": prev_grade != curr_grade if prev else False,
            "setup_changed": prev_setup != curr_setup if prev else False,
            "prev_setup": prev_setup,
            "window_ts": int(datetime.now().timestamp() * 1000),
        }
        return entry

    async def _sync_live_summary(self, dashboard_data_dir: Path, has_data: bool = True):
        """Generates a synthetic summary with consolidated M5 signals (1 winner per window)."""
        try:
            generated_at = datetime.now(tz=timezone.utc).isoformat()
            target_path = (dashboard_data_dir / f"summary_btcusdt_{self.mode}.json").absolute()

            if not has_data and target_path.exists():
                return
            
            # Select the single winning signal for this M5 window
            winner = self._select_m5_winning_signal()
            
            # Build consolidated entry with delta
            if winner:
                consolidated = self._build_m5_consolidated_entry(winner)
            else:
                consolidated = {}
            
            latest_tpfm = asdict(self._latest_tpfm_snapshot) if self._latest_tpfm_snapshot else {}
            
            # Patch 3 CONTENT GATE: Don't export summary if it's purely empty/initializing
            if not latest_tpfm and not consolidated and not self._m5_signal_history:
                return

            summary = {
                "instrument_key": self.instrument_key,
                "artifact_contract": {
                    "schema_version": "v2",
                    "mode": self.mode,
                    "run_id": self._runtime_run_id,
                    "generated_at": generated_at,
                    "window_end_ts": int(datetime.now().timestamp() * 1000),
                    "source": "live_engine"
                },
                "latest_tpfm": latest_tpfm,
                "top_signals": [consolidated] if consolidated else [],
                "m5_signal_history": self._m5_signal_history[-20:],  # Last 20 M5 consolidated entries
            }
            
            with target_path.open("w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ [Phase20B] Sync live summary failed: {e}")

    async def _sync_realtime_events(self, dashboard_data_dir: Path):
        """Phase 20-F: Export filtered high-fidelity realtime events for the Decision Cockpit."""
        try:
            src_log = Path("data/thesis/thesis_log.jsonl").absolute()
            if not src_log.exists():
                return
            
            filtered_records = []
            with src_log.open("r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        rec = json.loads(line)
                        event_type = rec.get("event_type", rec.get("type", ""))
                        
                        # Phase 20-F Quality Filter
                        should_keep = False
                        
                        # 1. Thesis state transitions are always kept
                        # Mapping stage_transition (new contract) to priority checks
                        if event_type == "stage_transition":
                            to_stage = rec.get("to_stage", "")
                            if to_stage in ("CONFIRMED", "ACTIONABLE", "INVALIDATED", "RESOLVED"):
                                should_keep = True
                        elif event_type in ("EVENT_THESIS_CONFIRMED", "EVENT_THESIS_ACTIONABLE", "EVENT_THESIS_INVALIDATED", "EVENT_THESIS_RESOLVED"):
                            should_keep = True
                        
                        # 2. Large liquidations or high intensity events
                        elif event_type == "EVENT_LIQUIDATION_LARGE" or rec.get("liquidation_quote", 0) > 20000:
                            should_keep = True
                        
                        # 3. High-quality TPFM transitions
                        elif event_type == "EVENT_TPFM_TRANSITION" or event_type == "tpfm_transition":
                            quality = rec.get("transition_quality", rec.get("quality", 0.0))
                            speed = rec.get("transition_speed", rec.get("speed", 0.0))
                            if quality >= 0.60 or speed >= 0.65:
                                should_keep = True
                        
                        # 4. Critical alerts or escalations
                        elif rec.get("severity") == "CRITICAL" or rec.get("should_escalate") is True:
                            should_keep = True
                            
                        if should_keep:
                            filtered_records.append(rec)
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            # Sort by timestamp (asc) and take last 50
            filtered_records.sort(key=lambda x: x.get("timestamp", 0))
            
            target = (dashboard_data_dir / f"realtime_events_{self.mode}.json").absolute()
            with target.open("w", encoding="utf-8") as f:
                json.dump(filtered_records[-50:], f, ensure_ascii=False)
        except Exception as e:
            # Silent failure for sync, don't break the engine
            print(f"⚠️ [Phase20B] Sync realtime events failed: {e}")

    def _emit_m5_timeline_records(self, snap: "TPFMSnapshot") -> None:
        """Phase 20-B: Emit market-timeline-anchored records for this M5 window."""
        try:
            import time as _time
            now_ms = int(_time.time() * 1000)
            run_id = self._runtime_run_id
            mode = self.mode
            symbol = snap.symbol
            venue = snap.venue
            market_ts = snap.window_end_ts
            stack = snap.stack_state

            # Parent Fallbacks (Patch 1: Use cached HTF data if parent_context is missing)
            m30_id = snap.parent_context.get("m30_state_id") if snap.parent_context else self._latest_m30_id
            h1_id = snap.parent_context.get("h1_state_id") if snap.parent_context else self._latest_h1_id
            h4_id = snap.parent_context.get("h4_state_id") if snap.parent_context else self._latest_h4_id
            h12_id = snap.parent_context.get("h12_state_id") if snap.parent_context else self._latest_h12_id
            d1_id = snap.parent_context.get("d1_state_id") if snap.parent_context else self._latest_d1_id

            m30_ts = snap.parent_context.get("m30_end_ts") if snap.parent_context else self._latest_m30_end_ts
            h1_ts = snap.parent_context.get("h1_end_ts") if snap.parent_context else self._latest_h1_end_ts
            h4_ts = snap.parent_context.get("h4_end_ts") if snap.parent_context else self._latest_h4_end_ts
            h12_ts = snap.parent_context.get("h12_end_ts") if snap.parent_context else self._latest_h12_end_ts
            d1_ts = snap.parent_context.get("d1_end_ts") if snap.parent_context else self._latest_d1_end_ts

            # 1. flow_frame_history (M5 state row)
            flow_bias = snap.continuation_bias if snap.continuation_bias in ("LONG", "SHORT") else "NEUTRAL"
            frame_row = {
                "frame_state_id": snap.snapshot_id,
                "run_id": run_id,
                "mode": mode,
                "symbol": symbol,
                "venue": venue,
                "frame": "M5",
                "market_ts": market_ts,
                "window_start_ts": snap.window_start_ts,
                "window_end_ts": snap.window_end_ts,
                "emitted_at_ts": now_ms,
                "ingested_at_ts": now_ms,
                "record_seq": 0,
                "is_final": 1,
                "source_kind": "tpfm_m5_snapshot",
                "source_ref_id": snap.snapshot_id,
                "snapshot_id": snap.snapshot_id,
                "pattern_id": snap.metadata.get("pattern_id", "") if snap.metadata else "",
                "stack_id": stack.stack_id if stack else f"{snap.snapshot_id}:stack",
                "open_px": snap.open_px,
                "high_px": snap.high_px,
                "low_px": snap.low_px,
                "close_px": snap.close_px,
                "volume_quote": snap.volume_quote,
                "matrix_cell": snap.matrix_cell,
                "matrix_alias_vi": snap.matrix_alias_vi,
                "flow_state_code": snap.flow_state_code,
                "pattern_code": snap.pattern_code,
                "pattern_phase": snap.pattern_phase,
                "sequence_id": snap.sequence_id,
                "sequence_signature": snap.sequence_signature,
                "sequence_length": snap.sequence_length,
                "flow_bias": flow_bias,
                "tempo_state": snap.tempo_state,
                "persistence_state": snap.persistence_state,
                "decision_posture": snap.decision_posture,
                "tradability_grade": snap.tradability_grade,
                "agreement_score": snap.agreement_score,
                "tradability_score": snap.tradability_score,
                "context_quality_score": snap.context_quality_score,
                "market_quality_score": snap.market_quality_score,
                "stack_signature": stack.stack_signature if stack else "",
                "stack_alignment": stack.stack_alignment if stack else "UNKNOWN",
                "stack_quality": stack.stack_quality if stack else 0.0,
                "parent_m30_end_ts": m30_ts,
                "parent_h1_end_ts": h1_ts,
                "parent_h4_end_ts": h4_ts,
                "parent_h12_end_ts": h12_ts,
                "parent_d1_end_ts": d1_ts,
                "health_state": snap.health_state,
                "metadata_json": json.dumps(snap.metadata if snap.metadata else {}, ensure_ascii=False),
            }
            self.store.save_flow_frame_state(frame_row)

            # 2. flow_timeline_event (STATE event)
            state_event_id = f"{snap.snapshot_id}:state"
            observed = snap.observed_facts[:3] if snap.observed_facts else []
            event_row = {
                "event_id": state_event_id,
                "run_id": run_id,
                "mode": mode,
                "symbol": symbol,
                "venue": venue,
                "frame": "M5",
                "market_ts": market_ts,
                "window_start_ts": snap.window_start_ts,
                "window_end_ts": snap.window_end_ts,
                "anchor_frame": "M5",
                "anchor_window_start_ts": snap.window_start_ts,
                "anchor_window_end_ts": snap.window_end_ts,
                "emitted_at_ts": now_ms,
                "ingested_at_ts": now_ms,
                "record_seq": 10,
                "is_final": 1,
                "event_type": "STATE",
                "signal_kind": "PATTERN",
                "severity": "INFO",
                "priority": 50,
                "snapshot_id": snap.snapshot_id,
                "transition_id": snap.transition_event.transition_id if snap.transition_event else "",
                "pattern_id": snap.metadata.get("pattern_id", "") if snap.metadata else "",
                "stack_id": stack.stack_id if stack else f"{snap.snapshot_id}:stack",
                "thesis_id": "",
                "matrix_cell": snap.matrix_cell,
                "matrix_alias_vi": snap.matrix_alias_vi,
                "flow_state_code": snap.flow_state_code,
                "pattern_code": snap.pattern_code,
                "pattern_phase": snap.pattern_phase,
                "sequence_signature": snap.sequence_signature,
                "decision_posture": snap.decision_posture,
                "tradability_grade": snap.tradability_grade,
                "action_label_vi": snap.action_plan_vi,
                "why_now_vi": " | ".join(observed),
                "invalid_if_vi": snap.invalid_if,
                "summary_vi": snap.decision_summary_vi,
                "parent_m30_end_ts": m30_ts,
                "parent_h1_end_ts": h1_ts,
                "parent_h4_end_ts": h4_ts,
                "parent_h12_end_ts": h12_ts,
                "parent_d1_end_ts": d1_ts,
                "metadata_json": "{}",
            }
            self.store.save_flow_timeline_event(event_row)

            # 3. flow_stack_history
            stack_id = stack.stack_id if stack else f"{snap.snapshot_id}:stack"
            
            # Phase 24: Construct comprehensive frames metadata for MTF strip
            # Try to build rich metadata for parent frames from snap context or cached IDs
            def _get_frame_meta(tf: str, state_id: str | None, end_ts: int | None) -> dict:
                if not state_id:
                    return {}

                # Prefer enriched aggregate metadata from the latest timeframe cache.
                cached = self._latest_frame_meta.get(tf, {})
                fallback_alias = f"{tf} đang đồng bộ"
                return {
                    "state_id": state_id,
                    "window_end_ts": end_ts or cached.get("window_end_ts"),
                    "alias_vi": cached.get("matrix_alias_vi") or fallback_alias,
                    "flow_bias": cached.get("flow_bias", "NEUTRAL"),
                    "tradability_grade": cached.get("tradability_grade") or "--",
                    "pattern_code": cached.get("pattern_code", ""),
                    "pattern_phase": cached.get("pattern_phase", ""),
                }

            stack_row = {
                "stack_id": stack_id,
                "run_id": run_id,
                "mode": mode,
                "symbol": symbol,
                "venue": venue,
                "market_ts": market_ts,
                "anchor_frame": "M5",
                "anchor_window_start_ts": snap.window_start_ts,
                "anchor_window_end_ts": snap.window_end_ts,
                "emitted_at_ts": now_ms,
                "ingested_at_ts": now_ms,
                "record_seq": 0,
                "is_final": 1,
                "m5_state_id": snap.snapshot_id,
                "m30_state_id": m30_id,
                "h1_state_id": h1_id,
                "h4_state_id": h4_id,
                "h12_state_id": h12_id,
                "d1_state_id": d1_id,
                "m5_end_ts": snap.window_end_ts,
                "m30_end_ts": m30_ts,
                "h1_end_ts": h1_ts,
                "h4_end_ts": h4_ts,
                "h12_end_ts": h12_ts,
                "d1_end_ts": d1_ts,
                "stack_signature": stack.stack_signature if stack else f"M5:{snap.matrix_cell}",
                "stack_alignment": stack.stack_alignment if stack else "MICRO_LEAD",
                "stack_conflict": stack.stack_conflict if stack else "UNKNOWN",
                "micro_vs_macro": stack.micro_vs_macro if stack else "UNKNOWN",
                "stack_pressure": stack.stack_pressure if stack else 0.0,
                "stack_quality": stack.stack_quality if stack else 1.0,
                "macro_bias": self._latest_frame_ids.get("MACRO_BIAS", "NEUTRAL"),
                "trigger_bias": flow_bias,
                "metadata_json": json.dumps({
                    "frames": {
                        "M5": {
                            "state_id": snap.snapshot_id,
                            "window_end_ts": snap.window_end_ts,
                            "matrix_cell": snap.matrix_cell,
                            "alias_vi": snap.matrix_alias_vi,
                            "pattern_code": snap.pattern_code,
                            "pattern_phase": snap.pattern_phase,
                            "tradability_grade": snap.tradability_grade,
                            "flow_bias": flow_bias,
                        },
                        "M30": _get_frame_meta("M30", m30_id, m30_ts),
                        "H1": _get_frame_meta("H1", h1_id, h1_ts),
                        "H4": _get_frame_meta("H4", h4_id, h4_ts),
                        "H12": _get_frame_meta("H12", h12_id, h12_ts),
                        "D1": _get_frame_meta("D1", d1_id, d1_ts),
                    }
                }, ensure_ascii=False),
            }
            self.store.save_flow_stack_history(stack_row)

        except Exception as e:
            # Patch 1: Show errors clearly in dev
            print(f"⚠️ [Phase20B] Emit timeline records failed: {e}")
            import traceback
            traceback.print_exc()

    def _export_flow_artifacts(self, dashboard_data_dir: Path) -> int:
        """Phase 20-B: Export flow history (Frames, Timeline, Stack) into tiered artifacts."""
        try:
            run_id = self._runtime_run_id
            mode = self.mode
            symbol = self.symbol
            
            now_ms = int(__import__("time").time() * 1000)

            def build_frame_item(r: dict) -> dict:
                return {
                    "run_id": export_run_id,
                    "mode": self.mode,
                    "symbol": self.symbol,
                    "frame_state_id": r.get("frame_state_id"),
                    "frame": r.get("frame", "M5"),
                    "market_ts": r.get("market_ts"),
                    "window": {"start_ts": r.get("window_start_ts"), "end_ts": r.get("window_end_ts")},
                    "source": {"kind": r.get("source_kind"), "ref_id": r.get("source_ref_id")},
                    "prices": {"open": r.get("open_px"), "high": r.get("high_px"), "low": r.get("low_px"), "close": r.get("close_px")},
                    "volume_quote": r.get("volume_quote"),
                    "flow": {
                        "matrix_cell": r.get("matrix_cell"), "matrix_alias_vi": r.get("matrix_alias_vi"),
                        "flow_state_code": r.get("flow_state_code"), "pattern_code": r.get("pattern_code"),
                        "pattern_phase": r.get("pattern_phase"), "flow_bias": r.get("flow_bias"),
                        "sequence_signature": r.get("sequence_signature"), "sequence_length": r.get("sequence_length"),
                        "tempo_state": r.get("tempo_state"), "persistence_state": r.get("persistence_state"),
                    },
                    "decision": {
                        "posture": r.get("decision_posture"), "tradability_grade": r.get("tradability_grade"),
                        "stack_alignment": r.get("stack_alignment"), "stack_quality": r.get("stack_quality"),
                        "action_label_vi": r.get("action_label_vi", ""),
                        "invalid_if_vi": r.get("invalid_if_vi", ""),
                    },
                    "parents": {
                        "m30_end_ts": r.get("parent_m30_end_ts"),
                        "h1_end_ts": r.get("parent_h1_end_ts"),
                        "h4_end_ts": r.get("parent_h4_end_ts"),
                    },
                    "health_state": r.get("health_state"),
                }

            def build_timeline_item(r: dict) -> dict:
                return {
                    "run_id": export_run_id,
                    "mode": self.mode,
                    "symbol": self.symbol,
                    "event_id": r.get("event_id"),
                    "market_ts": r.get("market_ts"),
                    "frame": r.get("frame"),
                    "window": {"start_ts": r.get("window_start_ts"), "end_ts": r.get("window_end_ts")},
                    "event_type": r.get("event_type"),
                    "signal_kind": r.get("signal_kind"),
                    "priority": r.get("priority"),
                    "context": {
                        "matrix_cell": r.get("matrix_cell"), "matrix_alias_vi": r.get("matrix_alias_vi"),
                        "pattern_code": r.get("pattern_code"), "pattern_phase": r.get("pattern_phase"),
                        "decision_posture": r.get("decision_posture"), "tradability_grade": r.get("tradability_grade"),
                    },
                    "message": {
                        "action_label_vi": r.get("action_label_vi", ""),
                        "why_now_vi": r.get("why_now_vi", ""),
                        "invalid_if_vi": r.get("invalid_if_vi", ""),
                        "summary_vi": r.get("summary_vi", ""),
                    },
                    "refs": {"snapshot_id": r.get("snapshot_id"), "thesis_id": r.get("thesis_id")},
                }

            def build_stack_item(r: dict) -> dict:
                import json as _json
                frames = {}
                try:
                    meta = _json.loads(r.get("metadata_json") or "{}")
                    frames = meta.get("frames", {})
                except Exception:
                    pass
                return {
                    "run_id": export_run_id,
                    "mode": self.mode,
                    "symbol": self.symbol,
                    "stack_id": r.get("stack_id"),
                    "market_ts": r.get("market_ts"),
                    "anchor": {"frame": r.get("anchor_frame"), "window_end_ts": r.get("anchor_window_end_ts")},
                    "stack_signature": r.get("stack_signature"),
                    "stack_alignment": r.get("stack_alignment"),
                    "stack_conflict": r.get("stack_conflict"),
                    "micro_vs_macro": r.get("micro_vs_macro"),
                    "stack_pressure": r.get("stack_pressure"),
                    "stack_quality": r.get("stack_quality"),
                    "macro_bias": r.get("macro_bias"),
                    "trigger_bias": r.get("trigger_bias"),
                    "frames": frames,
                }

            def _load_rows(target_run_id: str) -> tuple[list[dict], list[dict], list[dict]]:
                return (
                    self.store.load_flow_frames(target_run_id, self.mode, self.symbol, 100),
                    self.store.load_flow_timeline(target_run_id, self.mode, self.symbol, 50),
                    self.store.load_flow_stack(target_run_id, self.mode, self.symbol, 20),
                )

            # Load from store and export using engine's current mode
            export_run_id = run_id
            frame_rows, timeline_rows, stack_rows = _load_rows(export_run_id)

            if self.mode == "live" and not frame_rows:
                status_path = (dashboard_data_dir / "actions_status.json").absolute()
                summary_path = (dashboard_data_dir / f"summary_btcusdt_{self.mode}.json").absolute()
                candidate_run_ids: list[str] = []
                try:
                    if status_path.exists():
                        candidate_run_ids.append(
                            json.loads(status_path.read_text(encoding="utf-8")).get("artifact_run_id", "")
                        )
                except Exception:
                    pass
                try:
                    if summary_path.exists():
                        candidate_run_ids.append(
                            json.loads(summary_path.read_text(encoding="utf-8")).get("artifact_contract", {}).get("run_id", "")
                        )
                except Exception:
                    pass
                for fallback_run_id in candidate_run_ids:
                    if not fallback_run_id or fallback_run_id == run_id:
                        continue
                    fb_frames, fb_timeline, fb_stack = _load_rows(fallback_run_id)
                    if fb_frames:
                        export_run_id = fallback_run_id
                        frame_rows, timeline_rows, stack_rows = fb_frames, fb_timeline, fb_stack
                        print(f"ℹ️ [Phase20B] Backfill live flow artifacts from published run {fallback_run_id}")
                        break

            has_artifact_rows = bool(frame_rows or timeline_rows or stack_rows)
            self._last_exported_flow_run_id = export_run_id if has_artifact_rows else None
            frames_path = (dashboard_data_dir / f"flow_frames_{self.mode}.json").absolute()
            timeline_path = (dashboard_data_dir / f"flow_timeline_{self.mode}.json").absolute()
            stack_path = (dashboard_data_dir / f"flow_stack_{self.mode}.json").absolute()

            # Keep the last coherent live bundle visible until the new run has data.
            if (
                self.mode == "live"
                and not has_artifact_rows
                and frames_path.exists()
                and timeline_path.exists()
                and stack_path.exists()
            ):
                print("ℹ️ [Phase20B] Preserve previous live flow artifacts until new run has data")
                return 0

            artifact_contract = {
                "schema_version": "v1", "mode": self.mode,
                "run_id": export_run_id, "symbol": self.symbol,
                "generated_at_ts": now_ms,
            }

            frames_path.write_text(json.dumps({
                "artifact_contract": {**artifact_contract, "artifact_name": "flow_frames", "count": len(frame_rows)},
                "items": [build_frame_item(r) for r in frame_rows],
            }, ensure_ascii=False), encoding="utf-8")

            timeline_path.write_text(json.dumps({
                "artifact_contract": {**artifact_contract, "artifact_name": "flow_timeline", "count": len(timeline_rows)},
                "items": [build_timeline_item(r) for r in timeline_rows],
            }, ensure_ascii=False), encoding="utf-8")

            stack_path.write_text(json.dumps({
                "artifact_contract": {**artifact_contract, "artifact_name": "flow_stack", "count": len(stack_rows)},
                "items": [build_stack_item(r) for r in stack_rows],
            }, ensure_ascii=False), encoding="utf-8")

            return len(frame_rows)
        except Exception as e:
            print(f"❌ [Phase20B] Export flow artifacts failed: {e}")
            import traceback
            traceback.print_exc()
            return 0

    async def _sync_actions_status(self, dashboard_data_dir: Path, has_data: bool = True):
        """Update actions_status.json to reflect current engine state and mode."""
        try:
            now_ms = int(time.time() * 1000)
            generated_at = datetime.now(tz=timezone.utc).isoformat()
            
            # Publication Gate: Only update run_id if we have data or if it's already set to current
            target_status = (dashboard_data_dir / "actions_status.json").absolute()
            current_run_id = self._runtime_run_id
            
            if not has_data and target_status.exists():
                try:
                    with target_status.open("r", encoding="utf-8") as f:
                        old_status = json.load(f)
                        # Carry over old run_id if we aren't ready to publish new one yet
                        old_run_id = old_status.get("artifact_run_id", "")
                        if old_run_id and old_run_id != current_run_id:
                            old_frames = self.store.load_flow_frames(old_run_id, self.mode, self.symbol, 1)
                            if old_frames:
                                current_run_id = old_run_id
                            else:
                                summary_path = (dashboard_data_dir / f"summary_btcusdt_{self.mode}.json").absolute()
                                if summary_path.exists():
                                    summary_run_id = json.loads(summary_path.read_text(encoding="utf-8")).get("artifact_contract", {}).get("run_id", "")
                                    if summary_run_id:
                                        summary_frames = self.store.load_flow_frames(summary_run_id, self.mode, self.symbol, 1)
                                        if summary_frames:
                                            current_run_id = summary_run_id
                except Exception:
                    pass

            # Get top signal from existing state
            active_signals = [r.signal for r in self.thesis_state.values() if r.signal.stage in ["CONFIRMED", "ACTIONABLE"]]
            top_record_signal = None
            if active_signals:
                top_record_signal = sorted(active_signals, key=lambda s: (s.score, s.confidence), reverse=True)[0]
            
            top_signal = asdict(top_record_signal) if top_record_signal else {}
            
            latest_tpfm = asdict(self._latest_tpfm_snapshot) if self._latest_tpfm_snapshot else {}
            
            status_payload = {
                "last_run": generated_at,
                "last_scan_time": generated_at,
                "data_mode": "live",
                "live_enabled": True,
                "is_replay": False,
                "artifact_run_id": current_run_id,
                "artifact_window_end_ts": now_ms,
                "artifact_generated_at": generated_at,
                "latest_flow_grade": latest_tpfm.get("tradability_grade") or "D",
                "latest_transition": {},
                "top_signal": top_signal,
            }
            
            with target_status.open("w", encoding="utf-8") as f:
                json.dump(status_payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ [Phase20B] Sync actions status failed: {e}")

    def _map_sum_to_db_row(self, summary: dict, frame_state_id: str | None = None) -> dict:
        """Helper to map calculate_higher_frame_summary result to flow_frame_history row format."""
        from uuid import uuid4
        now_ms = int(__import__("time").time() * 1000)
        
        # Phase 20-H Enforcement: Capture meaningful bias for MTF stack
        flow_bias = summary.get("flow_bias", "NEUTRAL")
        if not flow_bias or flow_bias == "NEUTRAL":
            # Heuristic: use dominant_cell for bias if flow_bias is missing
            cell = summary.get("dominant_cell", "")
            if "SHORT" in cell: flow_bias = "SHORT"
            elif "LONG" in cell: flow_bias = "LONG"
        
        return {
            "frame_state_id": frame_state_id or f"sum:{summary['frame']}:{summary['window_end_ts']}:{uuid4().hex[:8]}",
            "run_id": self._runtime_run_id,
            "mode": "live",
            "symbol": summary["symbol"],
            "venue": self.venue,
            "frame": summary["frame"],
            "market_ts": summary["market_ts"],
            "window_start_ts": summary["window_start_ts"],
            "window_end_ts": summary["window_end_ts"],
            "emitted_at_ts": now_ms,
            "ingested_at_ts": now_ms,
            "record_seq": 0,
            "is_final": 1,
            "source_kind": "aggregator",
            "source_ref_id": f"m5_count:{summary.get('m5_count', 0)}",
            "open_px": summary["open_px"],
            "high_px": summary["high_px"],
            "low_px": summary["low_px"],
            "close_px": summary["close_px"],
            "volume_quote": summary["volume_quote"],
            "matrix_cell": summary["dominant_cell"],
            "matrix_alias_vi": summary["matrix_alias_vi"],
            "flow_state_code": f"{flow_bias}_REGIME", 
            "pattern_code": f"PERIODIC_{summary['frame']}",
            "pattern_phase": "CONSOLIDATED",
            "flow_bias": flow_bias,
            "decision_posture": "WAIT" if summary.get("agreement_score", 0) < 0.6 else "WATCH",
            "tradability_grade": summary["tradability_grade"],
            "agreement_score": summary["agreement_score"],
            "tradability_score": summary["tradability_score"],
            "market_quality_score": summary["market_quality_score"],
            "stack_quality": summary.get("persistence_score", 0.0),
            "metadata_json": json.dumps({
                "persistence": summary.get("persistence_score"),
                "m5_count": summary.get("m5_count")
            }, ensure_ascii=False),
        }

    def _check_higher_timeframe_aggregation(self, tpfm_snap: TPFMSnapshot) -> None:
        """Phase 20-E: Aggregates M5 snapshots into M30, H1, H4, H12, D1 and saves to SQLite."""
        try:
            self._tpfm_m5_roll_buffer.append(tpfm_snap)
            ts = tpfm_snap.window_end_ts
            m5_list = list(self._tpfm_m5_roll_buffer)
            
            # M30 Aggregation: Every 30m or 6 M5
            if ts % 1800000 < 300000:
                if len(m5_list) >= 6:
                    m30_sum = self.tpfm.calculate_m30_summary(m5_list[-6:])
                    row = self._map_sum_to_db_row(m30_sum)
                    self.store.save_flow_frame_state(row)
                    self._latest_frame_ids["M30"] = row["frame_state_id"]
                    self._latest_m30_id = row["frame_state_id"]
                    self._latest_m30_end_ts = m30_sum["window_end_ts"]
                    self._latest_frame_meta["M30"] = row
                    print(f"🪜 [MTF] Aggregated M30: {m30_sum['dominant_cell']} | Bias: {m30_sum['flow_bias']}")

            # H1 Aggregation: Every hour boundary or 12 M5
            if ts % 3600000 < 300000:
                if len(m5_list) >= 12:
                    h1_sum = self.tpfm.calculate_h1_summary(m5_list[-12:])
                    row = self._map_sum_to_db_row(h1_sum)
                    self.store.save_flow_frame_state(row)
                    self._latest_frame_ids["H1"] = row["frame_state_id"]
                    self._latest_m30_id = row["frame_state_id"] # Fallback if M30 not found
                    self._latest_h1_id = row["frame_state_id"]
                    self._latest_h1_end_ts = h1_sum["window_end_ts"]
                    self._latest_frame_ids["MACRO_BIAS"] = h1_sum["flow_bias"] # H1 defines macro bias for stack
                    self._latest_frame_meta["H1"] = row
                    print(f"🪜 [MTF] Aggregated H1: {h1_sum['dominant_cell']} | Bias: {h1_sum['flow_bias']}")

            # H4 Aggregation
            if ts % 14400000 < 300000: 
                if len(m5_list) >= 48:
                    h4_sum = self.tpfm.calculate_h4_summary(m5_list[-48:])
                    row = self._map_sum_to_db_row(h4_sum)
                    self.store.save_flow_frame_state(row)
                    self._latest_frame_ids["H4"] = row["frame_state_id"]
                    self._latest_h4_id = row["frame_state_id"]
                    self._latest_h4_end_ts = h4_sum["window_end_ts"]
                    self._latest_frame_meta["H4"] = row
                    print(f"🪜 [MTF] Aggregated H4: {h4_sum['dominant_cell']} | Bias: {h4_sum['flow_bias']}")

            # H12 Aggregation
            if ts % 43200000 < 300000:
                if len(m5_list) >= 144:
                    h12_sum = self.tpfm.calculate_h12_summary(m5_list[-144:])
                    row = self._map_sum_to_db_row(h12_sum)
                    self.store.save_flow_frame_state(row)
                    self._latest_frame_ids["H12"] = row["frame_state_id"]
                    self._latest_h12_id = row["frame_state_id"]
                    self._latest_h12_end_ts = h12_sum["window_end_ts"]
                    self._latest_frame_meta["H12"] = row
                    print(f"🪜 [MTF] Aggregated H12: {h12_sum['dominant_cell']} | Bias: {h12_sum['flow_bias']}")

            # D1 Aggregation
            if ts % 86400000 < 300000:
                if len(m5_list) >= 288:
                    d1_sum = self.tpfm.calculate_d1_summary(m5_list[-288:])
                    row = self._map_sum_to_db_row(d1_sum)
                    self.store.save_flow_frame_state(row)
                    self._latest_frame_ids["D1"] = row["frame_state_id"]
                    self._latest_d1_id = row["frame_state_id"]
                    self._latest_d1_end_ts = d1_sum["window_end_ts"]
                    self._latest_frame_meta["D1"] = row
                    print(f"🪜 [MTF] Aggregated D1: {d1_sum['dominant_cell']} | Bias: {d1_sum['flow_bias']}")

        except Exception as e:
            print(f"⚠️ [Phase20E] Higher frame aggregation failed: {e}")

    async def _init_book(self) -> bool:
        snapshot, error = try_fetch_depth_snapshot(symbol=self.symbol)
        
        # Fallback to Bybit if Binance is Geo-blocked (451)
        if snapshot is None and "451" in str(error):
            print(f"⚠️ Binance Geo-blocked (451). Thử khởi tạo Book qua Bybit...")
            try:
                result, b_err = try_fetch_bybit_depth(symbol=self.symbol)
                if result is None:
                    raise ValueError(f"Bybit fallback failed: {b_err}")
                
                # Map Bybit V5 structure to Binance-like for internal books
                self._depth.apply_snapshot(
                    bids=[(float(i[0]), float(i[1])) for i in result.get("b", [])],
                    asks=[(float(i[0]), float(i[1])) for i in result.get("a", [])],
                    last_update_id=int(result.get("u", 0)),
                )
                self.health.venue = "bybit"
                return True
            except Exception as e:
                print(f"⚠️ Bybit init failed ({e}). Thử khởi tạo Book qua OKX...")
                try:
                    result, o_err = try_fetch_okx_depth(symbol=self.symbol)
                    if result is None:
                        raise ValueError(f"OKX fallback failed: {o_err}")
                    
                    # Map OKX structure to Binance-like for internal books
                    self._depth.apply_snapshot(
                        bids=[(float(i[0]), float(i[1])) for i in result.get("bids", [])],
                        asks=[(float(i[0]), float(i[1])) for i in result.get("asks", [])],
                        last_update_id=int(result.get("ts", 0)),
                    )
                    self.health.venue = "okx"
                    return True
                except Exception as e2:
                    self.health.last_error = f"Tất cả fallback Bybit & OKX đều thất bại: {e2}"
                    return False

        if snapshot is None:
            self.health.last_error = error
            return False

        self._depth.apply_snapshot(
            bids=[(float(px), float(qty)) for px, qty in snapshot.get("bids", [])],
            asks=[(float(px), float(qty)) for px, qty in snapshot.get("asks", [])],
            last_update_id=int(snapshot["lastUpdateId"]),
        )
        return True

    def _has_m5_snapshot(self) -> bool:
        return bool(self._latest_tpfm_summary)

    def _live_floor_satisfied(
        self,
        *,
        started_monotonic: float,
        min_runtime_seconds: float | None,
        run_until_first_m5: bool,
    ) -> bool:
        if min_runtime_seconds is not None and (time.monotonic() - started_monotonic) < min_runtime_seconds:
            return False
        if run_until_first_m5 and not self._has_m5_snapshot():
            return False
        return True

    def _should_stop_live_session(
        self,
        *,
        processed: int,
        max_events: int | None,
        started_monotonic: float,
        min_runtime_seconds: float | None,
        run_until_first_m5: bool,
    ) -> bool:
        if max_events is None:
            if min_runtime_seconds is None and not run_until_first_m5:
                return False
            return self._live_floor_satisfied(
                started_monotonic=started_monotonic,
                min_runtime_seconds=min_runtime_seconds,
                run_until_first_m5=run_until_first_m5,
            )

        if processed < max_events:
            return False

        return self._live_floor_satisfied(
            started_monotonic=started_monotonic,
            min_runtime_seconds=min_runtime_seconds,
            run_until_first_m5=run_until_first_m5,
        )

    async def run_forever(
        self,
        max_events: int | None = None,
        *,
        min_runtime_seconds: float | None = None,
        run_until_first_m5: bool = False,
    ):
        started_at = datetime.now(tz=timezone.utc).isoformat()
        print(f"Khởi chạy loop cho {self.symbol}...")
        
        # Bootstrap DB schema
        await self.store.migrate_schema()
        
        session_started_monotonic = time.monotonic()
        if self.runtime_report_path is not None and self._runtime_lease is None:
            self._runtime_lease = acquire_live_runtime_lease(
                self.runtime_report_path,
                run_id=self._runtime_run_id,
            )

        retry_count = 0
        try:
            while retry_count < self.max_retries:
                try:
                    if not await self._init_book():
                        print("Lỗi khởi tạo sổ lệnh. Sẽ thử lại sau 10 giây...")
                        self._persist_runtime_artifact(
                            status="bootstrap_failed",
                            started_at=started_at,
                            processed=0,
                            event_counts={},
                        )
                        await asyncio.sleep(10)
                        retry_count += 1
                        continue

                    await self.store.acquire_writer_lock(
                        run_id=self._runtime_run_id,
                        pid=os.getpid(),
                        host=socket.gethostname()
                    )

                    # Schema Migration
                    if not self.store.schema_synced:
                        await self.store.migrate_schema()
                    
                    # Sync TPFM Probability Engine with historical results
                    await self.tpfm.sync_probability_stats(self.store)

                    futures_task = asyncio.create_task(self.futures_collector.stream_forever())

                    bybit_collector = BybitPublicCollector(topics=build_public_topics([self.symbol]))

                    async def bybit_loop():
                        async for msg in bybit_collector.stream_forever():
                            if msg.get("topic", "").startswith("publicTrade"):
                                for trade in msg.get("data", []):
                                    norm = normalize_bybit_trade(trade, f"bybit:{self.symbol}:perp")
                                    self._venue_trades["bybit"].append(norm)

                    bybit_task = asyncio.create_task(bybit_loop())

                    okx_inst_id = f"{self.symbol[:-4]}-USDT-SWAP" if self.symbol.endswith("USDT") else f"{self.symbol[:3]}-{self.symbol[3:]}-SWAP"
                    okx_collector = OkxPublicCollector(args=build_public_args([okx_inst_id]))

                    async def okx_loop():
                        async for msg in okx_collector.stream_forever():
                            if msg.get("arg", {}).get("channel") == "trades":
                                for trade in msg.get("data", []):
                                    norm = normalize_okx_trade(trade, f"okx:{self.symbol}:perp")
                                    self._venue_trades["okx"].append(norm)

                    okx_task = asyncio.create_task(okx_loop())

                    monitor = OutcomeMonitor(self.store)
                    monitor_task = asyncio.create_task(monitor.run_forever())
                    dashboard_sync_task = asyncio.create_task(self._dashboard_sync_loop())

                    # Primary Collector Assignment
                    streams = build_public_streams([self.symbol], use_agg_trade=self.use_agg_trade)
                    primary_collector = BinancePublicCollector(streams=streams)
                    use_bybit_primary = (self.health.venue == "bybit")
                    use_okx_primary = (self.health.venue == "okx")

                    if use_bybit_primary:
                        print(f"🛡️ Chuyển sang Bybit làm nguồn dữ liệu chính cho {self.symbol}.")
                        primary_collector = BybitPublicCollector(topics=build_public_topics([self.symbol]))
                    elif use_okx_primary:
                        print(f"🛡️ Chuyển sang OKX làm nguồn dữ liệu chính cho {self.symbol}.")
                        primary_collector = okx_collector

                    self._refresh_collector_health(
                        spot_collector=primary_collector if not (use_bybit_primary or use_okx_primary) else None,
                        bybit_collector=primary_collector if use_bybit_primary else bybit_collector,
                        okx_collector=primary_collector if use_okx_primary else okx_collector,
                    )

                    processed = 0
                    event_counts: dict[str, int] = {}
                    deferred_exit_notice_sent = False
                    self.health.connected = True
                    self.health.reconnect_count += 1
                    
                    if use_bybit_primary:
                        print(f"Đã kết nối Bybit Stream: {self.symbol}")
                    elif use_okx_primary:
                        print(f"Đã kết nối OKX Stream: {self.symbol}")
                    else:
                        print(f"Đã kết nối Binance Stream: {streams}")

                    status = "completed"
                    try:
                        iterator = primary_collector.stream_forever().__aiter__()
                        while not self._stop_event.is_set():
                            try:
                                enveloppe_or_msg = await asyncio.wait_for(iterator.__anext__(), timeout=self.watchdog_idle_seconds)
                            except asyncio.TimeoutError:
                                status = "watchdog_timeout"
                                self.health.last_error = (
                                    f"Watchdog không nhận được dữ liệu mới trong {self.watchdog_idle_seconds:.1f}s"
                                )
                                print(f"[WATCHDOG] {self.health.last_error}. Chủ động dừng loop để tự phục hồi.")
                                break
                            except StopAsyncIteration:
                                break

                            self.health.message_count = primary_collector.health_snapshot().message_count
                            self._refresh_collector_health(
                                spot_collector=primary_collector if not (use_bybit_primary or use_okx_primary) else None,
                                bybit_collector=primary_collector if use_bybit_primary else bybit_collector,
                                okx_collector=primary_collector if use_okx_primary else okx_collector,
                            )
                            self._last_message_monotonic = time.monotonic()
                            
                            normalized = None
                            if use_bybit_primary:
                                # Bybit Normalization (Topic based)
                                msg = enveloppe_or_msg
                                if msg.get("topic", "").startswith("publicTrade"):
                                    for trade in msg.get("data", []):
                                        normalized = normalize_bybit_trade(trade, f"bybit:{self.symbol}:perp")
                                        # Handle as multiple if needed, but here we process sequentially
                                        if normalized:
                                            self._trades.append(normalized)
                                            self._venue_trades["bybit"].append(normalized)
                                            self._last_trade_ts = normalized.venue_ts
                                            await self._process_trade_event(normalized)
                                            processed += 1
                                elif msg.get("topic", "").startswith("orderbook"):
                                    # Update depth if needed, currently we focus on trades for flow
                                    pass
                                continue # Already processed in sub-loop
                            elif use_okx_primary:
                                # OKX Normalization
                                msg = enveloppe_or_msg
                                if msg.get("arg", {}).get("channel") == "trades":
                                    for trade in msg.get("data", []):
                                        normalized = normalize_okx_trade(trade, f"okx:{self.symbol}:perp")
                                        if normalized:
                                            self._trades.append(normalized)
                                            self._venue_trades["okx"].append(normalized)
                                            self._last_trade_ts = normalized.venue_ts
                                            await self._process_trade_event(normalized)
                                            processed += 1
                                elif msg.get("arg", {}).get("channel") == "books":
                                    pass
                            else:
                                # Binance Normalization
                                envelope = enveloppe_or_msg
                                data = envelope.get("data", {})
                                if not isinstance(data, dict):
                                    continue

                                event_type = str(data.get("e", ""))
                                event_counts[event_type] = event_counts.get(event_type, 0) + 1

                                if event_type == "aggTrade":
                                    normalized = normalize_agg_trade(data, instrument_key=self.instrument_key)
                                elif event_type == "trade":
                                    normalized = normalize_trade(data, instrument_key=self.instrument_key)
                                elif event_type == "bookTicker":
                                    normalized = normalize_book_ticker(data, instrument_key=self.instrument_key)
                                elif event_type == "depthUpdate":
                                    norm_depth = normalize_depth_diff(data, instrument_key=self.instrument_key)
                                    self._depth.ingest_diff(norm_depth)

                            if isinstance(normalized, NormalizedTrade):
                                self._trades.append(normalized)
                                if not (use_bybit_primary or use_okx_primary):
                                    self._venue_trades["binance"].append(normalized)
                                self._last_trade_ts = normalized.venue_ts
                                await self._process_trade_event(normalized)

                            processed += 1
                            if processed % self.heartbeat_interval == 0:
                                gap = self._stale_gap_seconds()
                                health_snap = self.get_health_snapshot()
                                status_tag = ""
                                if health_snap.is_stale:
                                    status_tag = " ⚠️ [STALE]"
                                elif not health_snap.connected:
                                    status_tag = " ❌ [DISCONNECTED]"

                                print(
                                    f"💓{status_tag} Hệ thống đang chạy... Đã xử lý {processed} sự kiện | "
                                    f"message={self.health.message_count} | gap={gap:.1f}s"
                                )

                            if self._should_stop_live_session(
                                processed=processed,
                                max_events=max_events,
                                started_monotonic=session_started_monotonic,
                                min_runtime_seconds=min_runtime_seconds,
                                run_until_first_m5=run_until_first_m5,
                            ):
                                break

                    except Exception as exc:
                        status = "runtime_error"
                        print(f"Bắt lỗi trong loop: {exc}")
                        self.health.last_error = str(exc)
                        self.health.connected = False
                        print("Mất kết nối. Đang thử kết nối lại sau 10 giây...")
                        await asyncio.sleep(10)
                        retry_count += 1
                    finally:
                        self._refresh_collector_health(
                            spot_collector=primary_collector if "primary_collector" in locals() else None,
                            bybit_collector=bybit_collector if "bybit_collector" in locals() else None,
                            okx_collector=okx_collector if "okx_collector" in locals() else None,
                        )
                        monitor.stop()
                        monitor_task.cancel()
                        dashboard_sync_task.cancel()
                        if "futures_task" in locals():
                            futures_task.cancel()
                        if "bybit_task" in locals():
                            bybit_task.cancel()
                        if "okx_task" in locals():
                            okx_task.cancel()
                        self.health.connected = False
                        self._persist_runtime_artifact(
                            status=status,
                            started_at=started_at,
                            processed=processed,
                            event_counts=event_counts,
                        )

                except Exception as outer_exc:
                    print(f"Lỗi hệ thống nghiêm trọng: {outer_exc}")
                    await asyncio.sleep(10)
                    retry_count += 1

            if retry_count >= self.max_retries:
                print("Đã đạt giới hạn số lần thử lại. Dừng hệ thống.")
        finally:
            if self._runtime_lease is not None:
                await self.store.release_writer_lock(run_id=self._runtime_run_id, pid=os.getpid())
            
            release_live_runtime_lease(self._runtime_lease)
            self._runtime_lease = None

    async def _process_trade_event(self, trade: NormalizedTrade):
        from cfte.thesis.cards import render_trader_card
        
        if not self._depth or not self._depth.book.bids or not self._depth.book.asks:
            return

        self._trades = slice_trade_window(
            self._trades,
            end_ts=trade.venue_ts,
            lookback_seconds=self.trade_window_seconds,
            max_trades=self.max_window_trades,
        )

        # Phase 4: Get and pass hidden-flow context once per event.
        f_ctx = self.futures_collector.get_live_context(now_ms=trade.venue_ts)
        v_ctx = self._build_venue_context(trade.venue_ts)
        self._refresh_collector_health(now_ms=trade.venue_ts)
        self._update_context_health(
            futures_context=f_ctx,
            venue_context=v_ctx,
            event_ts=trade.venue_ts,
        )
        snapshot = build_tape_snapshot(
            instrument_key=self.instrument_key,
            order_book=self._depth.book,
            trades=self._trades,
            window_end_ts=trade.venue_ts,
            lookback_seconds=self.trade_window_seconds,
            max_window_trades=self.max_window_trades,
            futures_delta=f_ctx.get("futures_delta", 0.0),
            liquidation_vol=f_ctx.get("liquidation_vol", 0.0),
            liquidation_bias=f_ctx.get("liquidation_bias", "NEUTRAL"),
            venue_confirmation_state=v_ctx.get("venue_confirmation_state", "UNCONFIRMED"),
            leader_venue=v_ctx.get("leader_venue", "UNKNOWN"),
            before_book=self._previous_feature_book,
        )
        self._previous_feature_book = self._depth.book.clone()
        
        # Add live-specific metadata for Veto
        snapshot.metadata["gap_seconds"] = self._stale_gap_seconds()

        # TPFM M5 Window Management
        if self._tpfm_window_start_ts is None:
            self._tpfm_window_start_ts = trade.venue_ts
        
        self._tpfm_trades.append(trade)
        self._tpfm_snapshots.append(snapshot)
        
        # Check if 5 minutes have passed (300,000 ms)
        if trade.venue_ts - self._tpfm_window_start_ts >= 300000:
            print(f"📊 [TPFM] Đang tổng hợp snapshot M5 cho {self.symbol}...")
            # Calculate M5 Snapshot (Phase T1-T5 Refined)
            futures_context = dict(f_ctx)
            futures_context.update(self._build_venue_context(trade.venue_ts))

            tpfm_snap = self.tpfm.calculate_m5_snapshot(
                window_start_ts=self._tpfm_window_start_ts,
                window_end_ts=trade.venue_ts,
                trades=self._tpfm_trades,
                snapshots=self._tpfm_snapshots,
                active_theses=list(self.thesis_state.values()),
                futures_context=futures_context
            )
            tpfm_snap.run_id = self._runtime_run_id
            
            # Phase 14: Enrich Signals with TPFM Intelligence (Finding 5)
            for record in self.thesis_state.values():
                s = record.signal
                s.matrix_cell = tpfm_snap.matrix_cell
                s.flow_state = tpfm_snap.flow_state_code
                s.matrix_alias_vi = tpfm_snap.matrix_alias_vi
                s.decision_summary_vi = tpfm_snap.decision_summary_vi
                s.pattern_code = tpfm_snap.pattern_code
                s.pattern_phase = tpfm_snap.pattern_phase
                s.sequence_signature = tpfm_snap.sequence_signature
                if hasattr(tpfm_snap, "edge_profile") and tpfm_snap.edge_profile:
                    s.edge_score = tpfm_snap.edge_profile.edge_score
                    s.edge_confidence = tpfm_snap.edge_profile.confidence
            transition_event = tpfm_snap.transition_event
            
            if transition_event:
                self._last_transition_alias_vi = transition_event.transition_alias_vi
                self._last_transition_summary = {
                    "alias_vi": transition_event.transition_alias_vi,
                    "transition_code": transition_event.transition_code,
                    "transition_family": transition_event.transition_family,
                    "from_cell": transition_event.from_cell,
                    "to_cell": transition_event.to_cell,
                    "decision_shift": transition_event.decision_shift,
                    "timestamp": int(transition_event.timestamp),
                }
                print(
                    f"🔄 [TRANSITION] {transition_event.transition_alias_vi}: "
                    f"{transition_event.from_cell} -> {transition_event.to_cell} "
                    f"({transition_event.transition_code})"
                )
                await self.store.save_flow_transition(transition_event)
            
            # Update counts from state
            active_list = list(self.thesis_state.values())
            active_signals = [item.signal for item in active_list]
            tpfm_snap.active_thesis_count = len([signal for signal in active_signals if signal.stage in ACTIVE_STAGES])
            tpfm_snap.new_thesis_count = len([item for item in active_list if item.opened_ts >= self._tpfm_window_start_ts])
            tpfm_snap.actionable_count = len([signal for signal in active_signals if signal.stage == "ACTIONABLE"])
            tpfm_snap.invalidated_count = len([signal for signal in active_signals if signal.stage == "INVALIDATED"])
            tpfm_snap.resolved_count = len([signal for signal in active_signals if signal.stage == "RESOLVED"])
            setup_score_map: dict[str, float] = {}
            for signal in active_signals:
                setup_score_map[signal.setup] = max(setup_score_map.get(signal.setup, 0.0), signal.score)
            tpfm_snap.setup_score_map = setup_score_map
            tpfm_snap.dominant_setups = [setup for setup, _ in sorted(setup_score_map.items(), key=lambda item: item[1], reverse=True)[:3]]
            
            # persistence
            await self.store.save_tpfm_snapshot(tpfm_snap)
            
            # Phase 14: Final E2E Pattern Persistence
            pattern_event = tpfm_snap.metadata.get("pattern_event")
            if pattern_event:
                await self.store.save_flow_pattern_event(pattern_event)
            
            pattern_outcomes = tpfm_snap.metadata.get("pattern_outcomes", [])
            for outcome in pattern_outcomes:
                await self.store.save_pattern_outcome(outcome)

            # Optimized AI Insights for M5 (Gemini)
            now_ts = time.time()
            time_since_ai = now_ts - self._last_ai_brief_ts
            is_anomaly = tpfm_snap.should_escalate or tpfm_snap.tradability_grade in ["A", "S"]
            
            # Logic: Call AI every 1 hour OR if anomaly detected
            should_call_ai = (time_since_ai > 3600) or is_anomaly
            
            if self.ai_explainer and self.ai_explainer.api_key and should_call_ai:
                reason = "Chu kỳ 1h" if not is_anomaly else "🔥 Đột biến dòng tiền"
                print(f"🤖 [AI] Đang lấy nhận định ({reason}) cho {self.symbol}...")
                brief = self.ai_explainer.explain_m5_brief(tpfm_snap)
                tpfm_snap.flow_decision_brief = brief
                self._last_ai_brief_ts = now_ts
                print(f"💡 [AI BRIEF] {brief}")
            else:
                # Carry over previous reasoning to avoid empty box
                prev_brief = self._latest_tpfm_snapshot.flow_decision_brief if self._latest_tpfm_snapshot else ""
                tpfm_snap.flow_decision_brief = prev_brief or "☕ Đang theo dõi thị trường (Chờ chu kỳ nhận định tiếp theo)..."

            self._update_latest_tpfm_summary(tpfm_snap)
            self._latest_tpfm_snapshot = tpfm_snap
            if self._first_m5_seen_at is None:
                self._first_m5_seen_at = datetime.now(tz=timezone.utc).isoformat()
            
            # vNext: Standardized Trader-First Output
            print(render_tpfm_m5_card(tpfm_snap))
            
            # PHASE 2: Escalation logic (Optional now that card is always printed)
            if tpfm_snap.should_escalate:
                print(f"⚠️ [ESCALATION] {', '.join(tpfm_snap.escalation_reason)}")

            # Dashboard History & Export (Finalized with AI Brief)
            self._dashboard_m5_history.append(tpfm_snap)
            
            # Phase 19: Capture winning signal for this M5 window
            winner = self._select_m5_winning_signal()
            if winner:
                consolidated = self._build_m5_consolidated_entry(winner)
                self._m5_signal_history.append(consolidated)
                # Keep only last 50
                if len(self._m5_signal_history) > 50:
                    self._m5_signal_history = self._m5_signal_history[-50:]
                # Update prev for next window's delta
                self._prev_m5_winning_signal = winner
            
            # Phase 20-B: Emit market-timeline-anchored records to SQLite
            self._emit_m5_timeline_records(tpfm_snap)
            
            # Phase 20-E: Aggregation for higher frames
            self._check_higher_timeframe_aggregation(tpfm_snap)
            
            try:
                sfx = "_live" if self.mode == "live" else "_scan"
                m5_path = Path(f"data/review/tpfm_m5{sfx}.json")
                m5_path.parent.mkdir(parents=True, exist_ok=True)
                snapshots_json = [asdict(s) for s in self._dashboard_m5_history]
                m5_path.write_text(json.dumps(snapshots_json, ensure_ascii=False, indent=2), encoding="utf-8")
                # Also sync to docs/data immediately
                shutil.copy2(m5_path, Path(f"docs/data/tpfm_m5{sfx}.json").absolute())
            except Exception as e:
                print(f"⚠️ [DASHBOARD] Lỗi xuất M5 JSON: {e}")

            # PHASE 3: 30m Regime Synthesis
            self._tpfm_m5_buffer.append(tpfm_snap)
            if len(self._tpfm_m5_buffer) >= 6:
                print(f"🌀 [TPFM] Đang tổng hợp REGIME 30m cho {self.symbol}...")
                regime = self.tpfm.calculate_30m_regime(self._tpfm_m5_buffer)
                await self.store.save_tpfm_m30_regime(regime)
                
                # Phase 20-H Track M30 ID
                # regime row in flow_frame_history? No, it's a separate table for now.
                # But we should probably also save it to flow_frame_history for the MTF ladder.
                m30_id = f"frame:M30:{tpfm_snap.window_end_ts}"
                row = {
                    "frame_state_id": m30_id,
                    "run_id": self._runtime_run_id,
                    "mode": "live",
                    "symbol": self.symbol,
                    "venue": self.venue,
                    "frame": "M30",
                    "market_ts": tpfm_snap.window_end_ts,
                    "window_start_ts": self._tpfm_m5_buffer[0].window_start_ts,
                    "window_end_ts": tpfm_snap.window_end_ts,
                    "emitted_at_ts": int(datetime.now().timestamp() * 1000),
                    "ingested_at_ts": int(datetime.now().timestamp() * 1000),
                    "record_seq": 0,
                    "is_final": 1,
                    "source_kind": "regime_aggregator",
                    "source_ref_id": regime.regime_id,
                    "open_px": self._tpfm_m5_buffer[0].open_px,
                    "high_px": max(s.high_px for s in self._tpfm_m5_buffer),
                    "low_px": min(s.low_px for s in self._tpfm_m5_buffer),
                    "close_px": tpfm_snap.close_px,
                    "volume_quote": sum(s.volume_quote for s in self._tpfm_m5_buffer),
                    "matrix_cell": regime.dominant_cell,
                    "matrix_alias_vi": regime.dominant_regime,
                    "flow_state_code": regime.dominant_regime,
                    "pattern_code": "REGIME_30M",
                    "pattern_phase": "CONSOLIDATED",
                    "flow_bias": "LONG" if "ACCUMULATION" in regime.dominant_regime or "BUY" in regime.dominant_regime else ("SHORT" if "DISTRIBUTION" in regime.dominant_regime or "SELL" in regime.dominant_regime else "NEUTRAL"),
                    "decision_posture": regime.macro_posture,
                    "tradability_grade": "A" if regime.regime_persistence_score > 0.6 else "B",
                    "agreement_score": regime.agreement_score,
                    "tradability_score": regime.tradability_score,
                    "market_quality_score": regime.regime_persistence_score,
                    "stack_quality": regime.actionability_density,
                    "metadata_json": json.dumps({"persistence": regime.regime_persistence_score}, ensure_ascii=False),
                }
                self.store.save_flow_frame_state(row)
                self._latest_frame_ids["M30"] = m30_id
                self._latest_frame_meta["M30"] = row
                
                # Output Summary
                print(f"📝 [REGIME] {regime.dominant_regime} | Persistence: {regime.regime_persistence_score:.2f}")
                print(f"🛤️ Path: {' -> '.join([c.split('__')[0] for c in regime.transition_path])}")
                
                # PHASE 4: 4h Structural Report
                self._tpfm_m30_buffer.append(regime)
                if len(self._tpfm_m30_buffer) >= 8:
                    print(f"🏛️ [TPFM] Đang tổng hợp cấu trúc 4h cho {self.symbol}...")
                    structural_report = self.tpfm.calculate_4h_structural(self._tpfm_m30_buffer)
                    
                    # Call AI Explainer
                    analysis = self.ai_explainer.explain_4h_structural(structural_report)
                    structural_report.ai_analysis_vi = analysis
                    
                    await self.store.save_tpfm_4h_report(structural_report)
                    # Note: Structural report is H4, and we should track its ID too
                    # But calculate_h4_summary (periodic) also runs. We prioritize periodic for timeline.

                    print(f"🤖 [AI ANALYSIS]\n{analysis}")
                    
                    self._tpfm_m30_buffer = []

                self._tpfm_m5_buffer = []

            # Reset window
            self._tpfm_window_start_ts = trade.venue_ts
            self._tpfm_trades = []
            self._tpfm_snapshots = []

        # Phase 3: Update Outcome Realism
        await self.outcome_realism.update(snapshot)

        # Phase 5: Thesis as Adapter - Pass TPFM context
        signals = evaluate_setups(snapshot, tpfm_snapshot=self._latest_tpfm_snapshot)
        signals = self.veto_engine.apply(signals, snapshot)
        
        for signal in signals:
            prev_record = self.thesis_state.get(signal.thesis_id)
            prev_stage = prev_record.signal.stage if prev_record else None
            prev_score = self._last_alert_score.get(signal.thesis_id, 0.0)
            
            next_state, events = apply_signal_update(
                state=prev_record,
                signal=signal,
                event_ts=trade.venue_ts,
            )
            
            score_delta = abs(next_state.signal.score - prev_score)
            should_alert = self._should_alert_signal(
                prev_stage=prev_stage,
                next_stage=next_state.signal.stage,
                score=next_state.signal.score,
                score_delta=score_delta,
            )

            if should_alert:
                print(f"\n--- THÔNG BÁO TÍN HIỆU [{next_state.signal.thesis_id[:8]}] ---")
                print(render_trader_card(next_state.signal))
                self._last_alert_score[signal.thesis_id] = next_state.signal.score
            
            # Phase 3: Realism Entry for ACTIONABLE
            if next_state.signal.stage == "ACTIONABLE":
                self.outcome_realism.on_signal(next_state.signal, snapshot)

            # Persistence if stage changed or new thesis
            if next_state.signal.stage != prev_stage:
                # Get current entry price (mid-price or last trade)
                entry_px = snapshot.mid_px
                
                # Persistence
                await self.store.save_thesis(
                    next_state.signal, 
                    opened_ts=next_state.opened_ts, 
                    entry_px=entry_px,
                    closed_ts=next_state.closed_ts
                )
                
                # If this is a new thesis (prev_stage was None or it just opened)
                # initialize outcomes
                if prev_stage is None:
                    await self.store.init_outcomes(
                        thesis_id=signal.thesis_id,
                        horizons=self.horizons,
                        opened_ts=next_state.opened_ts
                    )

                if events:
                    print(self._render_event_summary(signal.thesis_id, events))

                for event in events:
                    await self.store.append_event(event)
                    if self.thesis_log is not None:
                        record = asdict(event)
                        record["flow"] = "live"
                        record["profile"] = self.ux.get("profile_name", "default")
                        record["symbol"] = self.symbol
                        record["instrument_key"] = self.instrument_key
                        self.thesis_log.append_record(record)
                # After processing all events for this signal, sync to dashboard
                await self._sync_to_dashboard()

            self.thesis_state[signal.thesis_id] = next_state

    def _stale_gap_seconds(self) -> float:
        if self._last_message_monotonic is None:
            return 0.0
        return time.monotonic() - self._last_message_monotonic

    def _persist_runtime_artifact(
        self,
        *,
        status: str,
        started_at: str,
        processed: int,
        event_counts: dict[str, int],
    ) -> None:
        if self.runtime_report_path is None:
            return
        f_health = self.futures_collector.get_health_report()
        collector_health_payload = {
            name: self._collector_snapshot_to_dict(snapshot)
            for name, snapshot in sorted(self._collector_health.items())
        }
        degraded_flags = self._runtime_degraded_flags()
        artifact = LiveRuntimeArtifact(
            symbol=self.symbol,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(tz=timezone.utc).isoformat(),
            processed_events=processed,
            event_counts=event_counts,
            pid=os.getpid(),
            run_id=self._runtime_run_id,
            owner_host=self._runtime_lease.host if self._runtime_lease is not None else None,
            lock_path=str(self._runtime_lease.lock_path) if self._runtime_lease is not None else None,
            lock_acquired_at=self._runtime_lease.acquired_at if self._runtime_lease is not None else None,
            reconnect_count=max(0, self.health.reconnect_count),
            message_count=self.health.message_count,
            idle_timeout_seconds=self.watchdog_idle_seconds,
            heartbeat_interval=self.heartbeat_interval,
            stale_gap_seconds=self._stale_gap_seconds(),
            last_error=self.health.last_error,
            last_trade_ts=self._last_trade_ts,
            futures_ws_latency_ms=f_health.get("ws_latency_ms"),
            futures_is_stale=f_health.get("is_stale", False),
            collector_health=collector_health_payload,
            context_health=dict(self._context_health),
            latest_tpfm=dict(self._latest_tpfm_summary),
            first_m5_seen_at=self._first_m5_seen_at,
            latest_transition=dict(self._last_transition_summary),
            latest_flow_grade=str(self._latest_tpfm_summary.get("tradability_grade", "")) or None,
            last_transition_alias_vi=self._last_transition_alias_vi,
            degraded_flags=degraded_flags,
        )
        persist_live_runtime_artifact(self.runtime_report_path, artifact, lease=self._runtime_lease)

    def stop(self):
        self._stop_event.set()

    def get_health_snapshot(self) -> CollectorHealthSnapshot:
        f_health = self.futures_collector.get_health_report()
        spot_snapshot = self._collector_health.get("binance_spot")
        notes = tuple(self._runtime_degraded_flags()[:4])
        
        # Combine spot and futures health
        connected = self.health.connected and f_health.get("connected", False)
        is_stale = f_health.get("is_stale", False) or self._stale_gap_seconds() > 15.0
        
        return CollectorHealthSnapshot(
            venue=self.health.venue,
            state="running" if (connected and not is_stale) else "degraded",
            connected=connected,
            connect_attempts=self.health.reconnect_count + 1,
            reconnect_count=max(0, self.health.reconnect_count),
            message_count=self.health.message_count,
            last_disconnect_reason=None,
            last_error=None if not self.health.last_error else build_error_surface(Exception(self.health.last_error)),
            latency_ms=f_health.get("ws_latency_ms"),
            is_stale=is_stale,
            last_message_ts=spot_snapshot.last_message_ts if spot_snapshot is not None else None,
            idle_gap_seconds=self._stale_gap_seconds(),
            notes=notes,
        )
