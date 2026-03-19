from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from cfte.books.binance_depth import BinanceDepthReconciler
from cfte.collectors.binance_public import BinancePublicCollector, build_public_streams, try_fetch_depth_snapshot
from cfte.collectors.binance_futures import BinanceFuturesCollector
from cfte.collectors.health import CollectorHealthSnapshot, build_error_surface
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade
from cfte.normalizers.binance import (
    normalize_agg_trade,
    normalize_book_ticker,
    normalize_depth_diff,
    normalize_trade,
)
from cfte.cli.reliability import LiveRuntimeArtifact, persist_live_runtime_artifact
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.storage.thesis_log import ThesisLogWriter
from cfte.live.outcome_monitor import OutcomeMonitor
from cfte.thesis.engines import evaluate_setups
from cfte.thesis.state import ThesisLifecycleRecord, apply_signal_update
from cfte.tpfm.engine import TPFMStateEngine
from cfte.tpfm.cards import render_tpfm_m5_card
from cfte.tpfm.ai_explainer import TPFMAIExplainer
from cfte.models.events import TapeSnapshot, NormalizedTrade


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
        self.max_retries = max(1, max_retries)
        self.health = LiveEngineHealth(venue="binance")
        self.thesis_state: dict[str, ThesisLifecycleRecord] = {}
        self._last_alert_score: dict[str, float] = {}
        self._depth = BinanceDepthReconciler(instrument_key=self.instrument_key)
        self._trades: list[NormalizedTrade] = []
        self._stop_event = asyncio.Event()
        self._last_message_monotonic: float | None = None
        self._last_trade_ts: int | None = None
        
        # TPFM State
        self.tpfm = TPFMStateEngine(symbol=self.symbol)
        self._tpfm_window_start_ts: int | None = None
        self._tpfm_trades: list[NormalizedTrade] = []
        self._tpfm_snapshots: list[TapeSnapshot] = []
        self._tpfm_m5_buffer: list[TPFMSnapshot] = []
        self._tpfm_m30_buffer: list[TPFM30mRegime] = []
        self.ai_explainer = TPFMAIExplainer()
        self.futures_collector = BinanceFuturesCollector(symbol=self.symbol)

    async def _init_book(self) -> bool:
        snapshot, error = try_fetch_depth_snapshot(symbol=self.symbol)
        if snapshot is None:
            self.health.last_error = error
            return False

        self._depth.apply_snapshot(
            bids=[(float(px), float(qty)) for px, qty in snapshot.get("bids", [])],
            asks=[(float(px), float(qty)) for px, qty in snapshot.get("asks", [])],
            last_update_id=int(snapshot["lastUpdateId"]),
        )
        return True

    async def run_forever(self, max_events: int | None = None):
        started_at = datetime.now(tz=timezone.utc).isoformat()
        print(f"Khởi chạy loop cho {self.symbol}...")
        
        retry_count = 0
        
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

                await self.store.migrate_schema()

                monitor = OutcomeMonitor(self.store)
                monitor_task = asyncio.create_task(monitor.run_forever())

                streams = build_public_streams([self.symbol], use_agg_trade=self.use_agg_trade)
                collector = BinancePublicCollector(streams=streams)

                processed = 0
                event_counts: dict[str, int] = {}
                self.health.connected = True
                self.health.reconnect_count += 1
                print(f"Đã kết nối Binance Stream: {streams}")

                status = "completed"
                try:
                    iterator = collector.stream_forever().__aiter__()
                    while not self._stop_event.is_set():
                        try:
                            envelope = await asyncio.wait_for(iterator.__anext__(), timeout=self.watchdog_idle_seconds)
                        except asyncio.TimeoutError:
                            status = "watchdog_timeout"
                            self.health.last_error = (
                                f"Watchdog không nhận được dữ liệu mới trong {self.watchdog_idle_seconds:.1f}s"
                            )
                            print(f"[WATCHDOG] {self.health.last_error}. Chủ động dừng loop để tự phục hồi.")
                            break
                        except StopAsyncIteration:
                            break

                        self.health.message_count = collector.health_snapshot().message_count
                        self._last_message_monotonic = time.monotonic()
                        data = envelope.get("data", {})
                        if not isinstance(data, dict):
                            continue

                        event_type = str(data.get("e", ""))
                        event_counts[event_type] = event_counts.get(event_type, 0) + 1
                        normalized = None

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
                            self._last_trade_ts = normalized.venue_ts
                            await self._process_trade_event(normalized)

                        processed += 1
                        if processed % self.heartbeat_interval == 0:
                            gap = self._stale_gap_seconds()
                            print(
                                f"💓 Hệ thống đang chạy... Đã xử lý {processed} sự kiện | "
                                f"message={self.health.message_count} | gap={gap:.1f}s"
                            )

                        if max_events and processed >= max_events:
                            break
                    
                    # If we exit normally (e.g. max_events or stop_event or watchdog break), 
                    # we check if we should break retry loop
                    if status == "completed" or self._stop_event.is_set():
                        break
                    else:
                        # For watchdog_timeout, we retry
                        retry_count += 1
                        print("Khởi động lại loop tìm kiếm dữ liệu mới...")
                        await asyncio.sleep(5)

                except Exception as exc:
                    status = "runtime_error"
                    print(f"Bắt lỗi trong loop: {exc}")
                    self.health.last_error = str(exc)
                    self.health.connected = False
                    print("Mất kết nối. Đang thử kết nối lại sau 10 giây...")
                    await asyncio.sleep(10)
                    retry_count += 1
                finally:
                    monitor.stop()
                    monitor_task.cancel()
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

    async def _process_trade_event(self, trade: NormalizedTrade):
        from cfte.thesis.cards import render_trader_card
        
        if not self._depth or not self._depth.book.bids or not self._depth.book.asks:
            return

        # Keep trade window bounded for personal use (e.g. last 1000 trades)
        if len(self._trades) > 1000:
            self._trades = self._trades[-1000:]

        snapshot = build_tape_snapshot(
            instrument_key=self.instrument_key,
            order_book=self._depth.book,
            trades=self._trades,
            window_start_ts=self._trades[0].venue_ts,
            window_end_ts=trade.venue_ts,
        )

        # TPFM M5 Window Management
        if self._tpfm_window_start_ts is None:
            self._tpfm_window_start_ts = trade.venue_ts
        
        self._tpfm_trades.append(trade)
        self._tpfm_snapshots.append(snapshot)
        
        # Check if 5 minutes have passed (300,000 ms)
        if trade.venue_ts - self._tpfm_window_start_ts >= 300000:
            print(f"📊 [TPFM] Đang tổng hợp snapshot M5 cho {self.symbol}...")
            # Calculate M5 Snapshot (Phase T1-T5 Refined)
            futures_context = self.futures_collector.get_live_context()
            
            tpfm_snap = self.tpfm.calculate_m5_snapshot(
                window_start_ts=self._tpfm_window_start_ts,
                window_end_ts=trade.venue_ts,
                trades=self._tpfm_trades,
                snapshots=self._tpfm_snapshots,
                active_theses=list(self.thesis_state.values()),
                futures_context=futures_context
            )
            
            # Update counts from state
            active_list = list(self.thesis_state.values())
            tpfm_snap.active_thesis_count = len(active_list)
            tpfm_snap.actionable_count = len([t for t in active_list if t.signal.stage == "ACTIONABLE"])
            
            # Persist
            await self.store.save_tpfm_snapshot(tpfm_snap)
            print(f"✅ [TPFM] Đã lưu M5: {tpfm_snap.matrix_cell}")
            
            # PHASE 2: Escalation Card
            if tpfm_snap.should_escalate:
                print(render_tpfm_m5_card(tpfm_snap))

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

        signals = evaluate_setups(snapshot)
        for signal in signals:
            prev_record = self.thesis_state.get(signal.thesis_id)
            prev_stage = prev_record.signal.stage if prev_record else None
            prev_score = self._last_alert_score.get(signal.thesis_id, 0.0)
            
            next_state, events = apply_signal_update(
                state=prev_record,
                signal=signal,
                event_ts=trade.venue_ts,
            )
            
            # Check if stage changed or important update
            should_alert = False
            if next_state.signal.stage != prev_stage:
                if self.ux.get("alert_on_stage_change", True):
                    should_alert = True
            
            score_delta = abs(next_state.signal.score - prev_score)
            if score_delta >= self.ux.get("alert_on_score_delta", 10.0):
                should_alert = True

            if should_alert:
                print(f"\n--- THÔNG BÁO TÍN HIỆU [{next_state.signal.thesis_id[:8]}] ---")
                print(render_trader_card(next_state.signal))
                self._last_alert_score[signal.thesis_id] = next_state.signal.score

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

                    await self.store.append_event(event)
                    print(f"📝 [EVENT] {signal.thesis_id[:8]} -> {event.to_stage}: {event.summary_vi}")
                    if self.thesis_log is not None:
                        self.thesis_log.append_record(
                            {
                                "flow": "live",
                                "event_type": event.event_type,
                                "thesis_id": event.thesis_id,
                                "from_stage": event.from_stage,
                                "to_stage": event.to_stage,
                                "event_ts": event.event_ts,
                                "summary_vi": event.summary_vi,
                                "score": event.score,
                                "confidence": event.confidence,
                                "symbol": self.symbol,
                                "instrument_key": self.instrument_key,
                                "setup": signal.setup,
                                "direction": signal.direction,
                            }
                        )

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
        artifact = LiveRuntimeArtifact(
            symbol=self.symbol,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(tz=timezone.utc).isoformat(),
            processed_events=processed,
            event_counts=event_counts,
            reconnect_count=max(0, self.health.reconnect_count),
            message_count=self.health.message_count,
            idle_timeout_seconds=self.watchdog_idle_seconds,
            heartbeat_interval=self.heartbeat_interval,
            stale_gap_seconds=self._stale_gap_seconds(),
            last_error=self.health.last_error,
            last_trade_ts=self._last_trade_ts,
        )
        persist_live_runtime_artifact(self.runtime_report_path, artifact)

    def stop(self):
        self._stop_event.set()

    def get_health_snapshot(self) -> CollectorHealthSnapshot:
        return CollectorHealthSnapshot(
            venue=self.health.venue,
            state="running" if self.health.connected else "degraded",
            connected=self.health.connected,
            connect_attempts=self.health.reconnect_count + 1,
            reconnect_count=max(0, self.health.reconnect_count),
            message_count=self.health.message_count,
            last_disconnect_reason=None,
            last_error=None if not self.health.last_error else build_error_surface(Exception(self.health.last_error)),
        )
