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
    ) -> None:
        self.symbol = symbol.upper()
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
        
        # TPFM State
        self.tpfm = TPFMStateEngine(symbol=self.symbol)
        self._tpfm_window_start_ts: int | None = None
        self._tpfm_trades: list[NormalizedTrade] = []
        self._tpfm_snapshots: list[TapeSnapshot] = []
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
        """
        try:
            # Dashboard target directory
            dashboard_data_dir = Path("docs/data")
            if not dashboard_data_dir.exists():
                dashboard_data_dir.mkdir(parents=True, exist_ok=True)

            # 1. Sync Thesis Log (.jsonl to .json array)
            src_log = Path("data/thesis/thesis_log.jsonl")
            if src_log.exists():
                try:
                    with src_log.open("r", encoding="utf-8") as f:
                        records = [json.loads(line) for line in f if line.strip()]
                    # Only sync last 100 for performance
                    target_log = dashboard_data_dir / "thesis_log.json"
                    with target_log.open("w", encoding="utf-8") as f:
                        json.dump(records[-100:], f, ensure_ascii=False)
                except Exception:
                    pass

            # 2. Sync TPFM M5
            m5_src = Path("data/review/tpfm_m5.json")
            if m5_src.exists():
                shutil.copy2(m5_src, dashboard_data_dir / "tpfm_m5.json")

            telemetry_map = {
                "data/review/daily_summary.json": "daily_summary.json",
                "data/review/health_status.json": "health_status.json",
                "data/replay/summary_btcusdt.json": "summary_btcusdt.json",
                "data/review/tpfm_m30.json": "tpfm_m30.json",
                "data/review/tpfm_4h.json": "tpfm_4h.json",
            }

            for src_rel, target_name in telemetry_map.items():
                src_p = Path(src_rel)
                if src_p.exists():
                    shutil.copy2(src_p, dashboard_data_dir / target_name)
                
        except Exception:
            # Silent failure for sync, don't break the engine
            pass

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
            
            # Persist
            await self.store.save_tpfm_snapshot(tpfm_snap)
            
            # Phase 14: Final E2E Pattern Persistence
            pattern_event = tpfm_snap.metadata.get("pattern_event")
            if pattern_event:
                await self.store.save_flow_pattern_event(pattern_event)
            
            pattern_outcomes = tpfm_snap.metadata.get("pattern_outcomes", [])
            for outcome in pattern_outcomes:
                await self.store.save_pattern_outcome(outcome)
            
            # Dashboard History & Export
            self._dashboard_m5_history.append(tpfm_snap)
            try:
                m5_path = Path("data/review/tpfm_m5.json")
                m5_path.parent.mkdir(parents=True, exist_ok=True)
                # Convert deque to list of dicts for JSON
                snapshots_json = [asdict(s) for s in self._dashboard_m5_history]
                m5_path.write_text(json.dumps(snapshots_json, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"⚠️ [DASHBOARD] Lỗi xuất M5 JSON: {e}")

            self._update_latest_tpfm_summary(tpfm_snap)
            self._latest_tpfm_snapshot = tpfm_snap
            if self._first_m5_seen_at is None:
                self._first_m5_seen_at = datetime.now(tz=timezone.utc).isoformat()
            
            # vNext: Standardized Trader-First Output
            print(render_tpfm_m5_card(tpfm_snap))
            
            # PHASE 2: Escalation logic (Optional now that card is always printed)
            if tpfm_snap.should_escalate:
                print(f"⚠️ [ESCALATION] {', '.join(tpfm_snap.escalation_reason)}")

            # PHASE 3: 30m Regime Synthesis
            self._tpfm_m5_buffer.append(tpfm_snap)
            if len(self._tpfm_m5_buffer) >= 6:
                print(f"🌀 [TPFM] Đang tổng hợp REGIME 30m cho {self.symbol}...")
                regime = self.tpfm.calculate_30m_regime(self._tpfm_m5_buffer)
                await self.store.save_tpfm_m30_regime(regime)
                
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
