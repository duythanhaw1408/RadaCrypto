from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator

from cfte.books.binance_depth import BinanceDepthReconciler
from cfte.collectors.binance_public import BinancePublicCollector, build_public_streams, try_fetch_depth_snapshot
from cfte.collectors.health import CollectorHealthSnapshot, build_error_surface
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade
from cfte.normalizers.binance import (
    normalize_agg_trade,
    normalize_book_ticker,
    normalize_depth_diff,
    normalize_trade,
)
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.live.outcome_monitor import OutcomeMonitor
from cfte.thesis.engines import evaluate_setups
from cfte.thesis.state import ThesisLifecycleRecord, apply_signal_update


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
    ) -> None:
        self.symbol = symbol.upper()
        self.instrument_key = f"BINANCE:{self.symbol}:SPOT"
        self.db_path = db_path
        self.use_agg_trade = use_agg_trade
        self.horizons = horizons or ["1h", "4h", "24h"]
        self.ux = {}  # Will be set by run-live
        self.store = ThesisSQLiteStore(db_path)
        self.health = LiveEngineHealth(venue="binance")
        self.thesis_state: dict[str, ThesisLifecycleRecord] = {}
        self._last_alert_score: dict[str, float] = {}
        self._depth = BinanceDepthReconciler(instrument_key=self.instrument_key)
        self._trades: list[NormalizedTrade] = []
        self._stop_event = asyncio.Event()

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
        print(f"Khởi chạy loop cho {self.symbol}...")
        if not await self._init_book():
            print("Lỗi khởi tạo sổ lệnh.")
            return

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

        try:
            async for envelope in collector.stream_forever():
                if self._stop_event.is_set():
                    break

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
                    await self._process_trade_event(normalized)

                processed += 1
                if processed % 1000 == 0:
                    print(f"💓 Hệ thống đang chạy... Đã xử lý {processed} sự kiện.")
                
                if max_events and processed >= max_events:
                    break
        except Exception as exc:
            print(f"Bắt lỗi trong loop: {exc}")
            self.health.last_error = str(exc)
            self.health.connected = False
        finally:
            monitor.stop()
            self.health.connected = False

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

                for event in events:
                    await self.store.append_event(event)

            self.thesis_state[signal.thesis_id] = next_state

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
