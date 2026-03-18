from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedDepthDiff, NormalizedTrade, ThesisSignal
from cfte.replay.adapters import ReplayBookSnapshot, ReplayEvent
from cfte.thesis.engines import evaluate_setups


@dataclass(slots=True)
class ReplayRunResult:
    instrument_key: str
    event_count: int
    thesis_count: int
    feature_windows: int
    fingerprint: str
    thesis_events: list[ThesisSignal]


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
        }
        for s in signals
    ]
    payload = json.dumps(serialized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_replay(events: list[ReplayEvent]) -> ReplayRunResult:
    if not events:
        raise ValueError("Replay event list is empty")

    ordered_events = [event for _, event in sorted(enumerate(events), key=lambda item: (item[1].venue_ts, item[0]))]

    book: LocalBook | None = None
    instrument_key: str | None = None
    trades: list[NormalizedTrade] = []
    thesis_events: list[ThesisSignal] = []
    feature_windows = 0

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
            feature_windows += 1
            snapshot = build_tape_snapshot(
                instrument_key=instrument_key,
                order_book=book,
                trades=trades,
                window_start_ts=trades[0].venue_ts,
                window_end_ts=payload.venue_ts,
            )
            thesis_events.extend(evaluate_setups(snapshot))

    if instrument_key is None:
        raise ValueError("No instrument_key found in replay events")

    return ReplayRunResult(
        instrument_key=instrument_key,
        event_count=len(ordered_events),
        thesis_count=len(thesis_events),
        feature_windows=feature_windows,
        fingerprint=_fingerprint_signals(thesis_events),
        thesis_events=thesis_events,
    )


def persist_replay_summary(result: ReplayRunResult, output_path: str | Path) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "instrument_key": result.instrument_key,
        "event_count": result.event_count,
        "feature_windows": result.feature_windows,
        "thesis_count": result.thesis_count,
        "fingerprint": result.fingerprint,
        "top_signals": [asdict(s) for s in result.thesis_events[:5]],
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
