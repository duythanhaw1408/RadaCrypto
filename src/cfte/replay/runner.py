from __future__ import annotations

from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade, ThesisSignal
from cfte.thesis.engines import evaluate_setups

def replay_from_events(
    instrument_key: str,
    snapshot_bids: list[tuple[float, float]],
    snapshot_asks: list[tuple[float, float]],
    trades: list[NormalizedTrade],
) -> list[ThesisSignal]:
    book = LocalBook(instrument_key)
    book.apply_snapshot(snapshot_bids, snapshot_asks, seq_id=1)
    if not trades:
        return []
    snap = build_tape_snapshot(
        instrument_key=instrument_key,
        order_book=book,
        trades=trades,
        window_start_ts=trades[0].venue_ts,
        window_end_ts=trades[-1].venue_ts,
    )
    return evaluate_setups(snap)
