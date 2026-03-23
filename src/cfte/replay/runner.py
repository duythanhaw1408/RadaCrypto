from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from cfte.books.local_book import LocalBook
from cfte.features.tape import (
    DEFAULT_MAX_WINDOW_TRADES,
    DEFAULT_TRADE_WINDOW_SECONDS,
    build_tape_snapshot,
    slice_trade_window,
)
from cfte.models.events import NormalizedDepthDiff, NormalizedTrade, ThesisSignal
from cfte.replay.adapters import ReplayBookSnapshot, ReplayEvent
from cfte.thesis.engines import evaluate_setups, _apply_flow_context_to_signals
from cfte.thesis.state import ThesisEventRecord, ThesisLifecycleRecord, apply_signal_update, close_signal_state
from cfte.tpfm.engine import TPFMStateEngine
from cfte.storage.sqlite_writer import ThesisSQLiteStore


@dataclass(slots=True)
class ReplayRunResult:
    instrument_key: str
    event_count: int
    thesis_count: int
    feature_windows: int
    fingerprint: str
    thesis_events: list[ThesisSignal]
    thesis_state: dict[str, ThesisLifecycleRecord]
    thesis_event_history: list[ThesisEventRecord]
    run_id: str
    latest_tpfm_snapshot: Optional['TPFMSnapshot'] = None


_SIGNAL_STAGE_RANK = {
    "ACTIONABLE": 3,
    "CONFIRMED": 2,
    "WATCHLIST": 1,
    "DETECTED": 0,
    "INVALIDATED": -1,
    "RESOLVED": -2,
}


def _fingerprint_signals(signals: list[ThesisSignal]) -> str:
    serialized = [
        {
            "thesis_id": s.thesis_id,
            "setup": s.setup,
            "direction": s.direction,
            "stage": s.stage,
            "score": s.score,
            "confidence": s.confidence,
            "coverage": s.coverage,
            "why_now": s.why_now,
            "conflicts": s.conflicts,
            "invalidation": s.invalidation,
            "entry_style": s.entry_style,
            "targets": s.targets,
            "timeframe": s.timeframe,
            "regime_bucket": s.regime_bucket,
            "flow_state": s.flow_state,
            "matrix_cell": s.matrix_cell,
            "matrix_alias_vi": s.matrix_alias_vi,
            "tradability_grade": s.tradability_grade,
            "decision_posture": s.decision_posture,
            "flow_alignment_score": s.flow_alignment_score,
            "pattern_code": s.pattern_code,
            "pattern_phase": s.pattern_phase,
            "sequence_signature": s.sequence_signature,
        }
        for s in signals
    ]
    payload = json.dumps(serialized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def select_top_signals(signals: list[ThesisSignal], limit: int = 5) -> list[ThesisSignal]:
    best_by_thesis: dict[str, ThesisSignal] = {}
    for signal in signals:
        current = best_by_thesis.get(signal.thesis_id)
        if current is None:
            best_by_thesis[signal.thesis_id] = signal
            continue

        current_rank = (
            _SIGNAL_STAGE_RANK.get(current.stage, -10),
            current.flow_alignment_score,
            current.score,
            current.confidence,
        )
        candidate_rank = (
            _SIGNAL_STAGE_RANK.get(signal.stage, -10),
            signal.flow_alignment_score,
            signal.score,
            signal.confidence,
        )
        if candidate_rank > current_rank:
            best_by_thesis[signal.thesis_id] = signal

    ranked = sorted(
        best_by_thesis.values(),
        key=lambda item: (_SIGNAL_STAGE_RANK.get(item.stage, -10), item.flow_alignment_score, item.score, item.confidence),
        reverse=True,
    )
    return ranked[:limit]


def run_replay(
    events: list[ReplayEvent],
    db_path: str | Path | None = None,
    *,
    trade_window_seconds: float = DEFAULT_TRADE_WINDOW_SECONDS,
    max_window_trades: int = DEFAULT_MAX_WINDOW_TRADES,
) -> ReplayRunResult:
    if not events:
        raise ValueError("Replay event list is empty")

    ordered_events = [event for _, event in sorted(enumerate(events), key=lambda item: (item[1].venue_ts, item[0]))]

    book: LocalBook | None = None
    instrument_key: str | None = None
    trades: list[NormalizedTrade] = []
    thesis_events: list[ThesisSignal] = []
    thesis_state: dict[str, ThesisLifecycleRecord] = {}
    thesis_event_history: list[ThesisEventRecord] = []
    feature_windows = 0
    tpfm = TPFMStateEngine()
    store = ThesisSQLiteStore(db_path) if db_path else None
    if store:
        import asyncio
        asyncio.run(store.migrate_schema())
    previous_feature_book: LocalBook | None = None
    
    tpfm_trades: list[NormalizedTrade] = []
    tpfm_snapshots: list[TapeSnapshot] = []
    tpfm_window_start_ts: int | None = None
    tpfm_m5_buffer = []
    tpfm_m30_buffer = []
    latest_tpfm_snapshot = None
    all_m5_snapshots: list[Any] = []
    current_run_id = uuid4().hex

    for event in ordered_events:
        if event.event_type == "book_snapshot":
            payload = event.payload
            if not isinstance(payload, ReplayBookSnapshot):
                raise TypeError("book_snapshot payload must be ReplayBookSnapshot")
            instrument_key = payload.instrument_key
            book = LocalBook(instrument_key)
            book.apply_snapshot(payload.bids, payload.asks, seq_id=payload.seq_id)
            continue

        if book is None or instrument_key is None:
            raise ValueError("Replay must start with book_snapshot before diff/trade events")

        if event.event_type == "depth_diff":
            payload = event.payload
            if not isinstance(payload, NormalizedDepthDiff):
                raise TypeError("depth_diff payload must be NormalizedDepthDiff")
            if payload.instrument_key != instrument_key:
                raise ValueError("Replay event instrument_key mismatch")
            book.apply_diff(payload.bid_updates, payload.ask_updates, seq_id=payload.final_update_id)
            continue

        if event.event_type == "trade":
            payload = event.payload
            if not isinstance(payload, NormalizedTrade):
                raise TypeError("trade payload must be NormalizedTrade")
            if payload.instrument_key != instrument_key:
                raise ValueError("Replay event instrument_key mismatch")
            trades.append(payload)
            trades = slice_trade_window(
                trades,
                end_ts=payload.venue_ts,
                lookback_seconds=trade_window_seconds,
                max_trades=max_window_trades,
            )
            feature_windows += 1
            snapshot = build_tape_snapshot(
                instrument_key=instrument_key,
                order_book=book,
                trades=trades,
                window_start_ts=trades[0].venue_ts,
                window_end_ts=payload.venue_ts,
                lookback_seconds=trade_window_seconds,
                max_window_trades=max_window_trades,
                before_book=previous_feature_book,
            )
            previous_feature_book = book.clone()
            evaluated_signals = evaluate_setups(snapshot, tpfm_snapshot=latest_tpfm_snapshot)
            thesis_events.extend(evaluated_signals)
            for signal in evaluated_signals:
                next_state, state_events = apply_signal_update(
                    state=thesis_state.get(signal.thesis_id),
                    signal=signal,
                    event_ts=payload.venue_ts,
                )
                thesis_state[signal.thesis_id] = next_state
                thesis_event_history.extend(state_events)

            # TPFM Integration in Replay
            if tpfm_window_start_ts is None:
                tpfm_window_start_ts = payload.venue_ts
            
            tpfm_trades.append(payload)
            tpfm_snapshots.append(snapshot)
            
            # Check if 5 minutes have passed (300,000 ms)
            if payload.venue_ts - tpfm_window_start_ts >= 300000:
                m5_snap = tpfm.calculate_m5_snapshot(
                    window_start_ts=tpfm_window_start_ts,
                    window_end_ts=payload.venue_ts,
                    trades=tpfm_trades,
                    snapshots=tpfm_snapshots,
                    active_theses=list(thesis_state.values()),
                    futures_context=None # No futures in historical replay for now
                )
                m5_snap.run_id = current_run_id
                
                # Phase 14: Enrich Signals with TPFM Intelligence (Finding 5)
                for record in thesis_state.values():
                    s = record.signal
                    s.matrix_cell = m5_snap.matrix_cell
                    s.flow_state = m5_snap.flow_state_code
                    s.matrix_alias_vi = m5_snap.matrix_alias_vi
                    s.decision_summary_vi = m5_snap.decision_summary_vi
                    s.pattern_code = m5_snap.pattern_code
                    s.pattern_phase = m5_snap.pattern_phase
                    s.sequence_signature = m5_snap.sequence_signature
                    if hasattr(m5_snap, "edge_profile") and m5_snap.edge_profile:
                        s.edge_score = m5_snap.edge_profile.edge_score
                        s.edge_confidence = m5_snap.edge_profile.confidence
                transition_event = m5_snap.transition_event
                latest_tpfm_snapshot = m5_snap

                if store:
                    asyncio.run(store.save_tpfm_snapshot(m5_snap))
                all_m5_snapshots.append(m5_snap)
                
                if transition_event and not store:
                     # Log to stdout if no store, consistent with live print
                     print(
                         f"🔄 [REPLAY-TRANSITION] {transition_event.transition_alias_vi}: "
                         f"{transition_event.from_cell} -> {transition_event.to_cell}"
                     )
                
                if transition_event:
                    print(
                        f"🔄 [TRANSITION] {transition_event.transition_alias_vi}: "
                        f"{transition_event.from_cell} -> {transition_event.to_cell}"
                    )
                    if store:
                        asyncio.run(store.save_flow_transition(transition_event))
                
                # Phase 14: Final E2E Pattern Persistence
                if store:
                    pattern_ev = m5_snap.metadata.get("pattern_event")
                    if pattern_ev:
                        asyncio.run(store.save_flow_pattern_event(pattern_ev))
                    
                    pattern_outcomes = m5_snap.metadata.get("pattern_outcomes", [])
                    for outcome in pattern_outcomes:
                        asyncio.run(store.save_pattern_outcome(outcome))
                
                tpfm_m5_buffer.append(m5_snap)
                if len(tpfm_m5_buffer) >= 6:
                    regime = tpfm.calculate_30m_regime(tpfm_m5_buffer)
                    if store:
                        asyncio.run(store.save_tpfm_m30_regime(regime))
                    
                    tpfm_m30_buffer.append(regime)
                    if len(tpfm_m30_buffer) >= 8:
                        struct = tpfm.calculate_4h_structural(tpfm_m30_buffer)
                        if store:
                            asyncio.run(store.save_tpfm_4h_report(struct))
                        tpfm_m30_buffer = []
                
                # Reset 5m window
                tpfm_window_start_ts = payload.venue_ts
                tpfm_trades = []
                tpfm_snapshots = []

    # Final TPFM flush at end of replay (Phase T1-T5 Refinement)
    if tpfm_trades and store:
        m5_snap = tpfm.calculate_m5_snapshot(
            window_start_ts=tpfm_window_start_ts or ordered_events[0].venue_ts,
            window_end_ts=ordered_events[-1].venue_ts,
            trades=tpfm_trades,
            snapshots=tpfm_snapshots,
            active_theses=list(thesis_state.values()),
            futures_context=None
        )
        m5_snap.run_id = current_run_id
        
        latest_tpfm_snapshot = m5_snap
        all_m5_snapshots.append(m5_snap)

        # Phase 14: Final E2E Pattern Persistence
        pattern_ev = m5_snap.metadata.get("pattern_event")
        if pattern_ev:
            asyncio.run(store.save_flow_pattern_event(pattern_ev))
        
        final_outcomes = tpfm.flush_all_pending_outcomes(m5_snap)
        for outcome in final_outcomes:
            asyncio.run(store.save_pattern_outcome(outcome))
        
        # Flush M30/4H even if thresholds not met (Phase T1-T5 Refinement)
        tpfm_m5_buffer.append(m5_snap)
        if tpfm_m5_buffer:
            regime = tpfm.calculate_30m_regime(tpfm_m5_buffer)
            asyncio.run(store.save_tpfm_m30_regime(regime))
            tpfm_m30_buffer.append(regime)
            
            if tpfm_m30_buffer:
                struct = tpfm.calculate_4h_structural(tpfm_m30_buffer)
                asyncio.run(store.save_tpfm_4h_report(struct))

    if instrument_key is None:
        raise ValueError("No instrument_key found in replay events")

    for thesis_id, state in list(thesis_state.items()):
        if state.signal.stage == "ACTIONABLE":
            next_state, state_event = close_signal_state(state=state, next_stage="RESOLVED", event_ts=ordered_events[-1].venue_ts)
            thesis_state[thesis_id] = next_state
            if state_event is not None:
                thesis_event_history.append(state_event)

    # Export all M5 snapshots to JSON for dashboard scan mode
    try:
        m5_path = Path("data/review/tpfm_m5_scan.json")
        m5_path.parent.mkdir(parents=True, exist_ok=True)
        snapshots_json = [asdict(s) for s in all_m5_snapshots]
        m5_path.write_text(json.dumps(snapshots_json, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ [REPLAY] Lỗi xuất M5 JSON: {e}")

    return ReplayRunResult(
        instrument_key=instrument_key,
        event_count=len(ordered_events),
        thesis_count=len(thesis_events),
        feature_windows=feature_windows,
        fingerprint=_fingerprint_signals(thesis_events),
        thesis_events=thesis_events,
        thesis_state=thesis_state,
        thesis_event_history=thesis_event_history,
        run_id=current_run_id,
        latest_tpfm_snapshot=latest_tpfm_snapshot,
    )


def persist_replay_summary(result: ReplayRunResult, output_path: str | Path) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    latest_tpfm = asdict(result.latest_tpfm_snapshot) if result.latest_tpfm_snapshot else {}
    summary = {
        "instrument_key": result.instrument_key,
        "event_count": result.event_count,
        "feature_windows": result.feature_windows,
        "thesis_count": result.thesis_count,
        "fingerprint": result.fingerprint,
        "artifact_contract": {
            "mode": "scan",
            "run_id": result.run_id,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "window_end_ts": latest_tpfm.get("window_end_ts", 0),
            "source": "replay_scan",
        },
        "latest_tpfm": latest_tpfm,
        "top_signals": [
            asdict(_apply_flow_context_to_signals([s], result.latest_tpfm_snapshot)[0]) 
            for s in select_top_signals(result.thesis_events, limit=5)
        ],
    }
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def render_replay_summary_vi(result: ReplayRunResult) -> str:
    return (
        f"Replay hoàn tất cho {result.instrument_key}: "
        f"{result.event_count} sự kiện, "
        f"{result.feature_windows} cửa sổ đặc trưng, "
        f"{result.thesis_count} tín hiệu thesis, "
        f"dấu vân tay {result.fingerprint[:12]}."
    )


def replay_from_events(
    instrument_key: str,
    snapshot_bids: list[tuple[float, float]],
    snapshot_asks: list[tuple[float, float]],
    trades: list[NormalizedTrade],
) -> list[ThesisSignal]:
    events: list[ReplayEvent] = [
        ReplayEvent(
            event_type="book_snapshot",
            venue_ts=trades[0].venue_ts if trades else 0,
            payload=ReplayBookSnapshot(
                instrument_key=instrument_key,
                bids=snapshot_bids,
                asks=snapshot_asks,
                seq_id=1,
                venue_ts=trades[0].venue_ts if trades else 0,
            ),
        )
    ]
    events.extend(ReplayEvent(event_type="trade", venue_ts=t.venue_ts, payload=t) for t in trades)
    return run_replay(events).thesis_events
