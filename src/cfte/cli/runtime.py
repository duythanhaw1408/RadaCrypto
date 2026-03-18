from __future__ import annotations

from dataclasses import asdict, dataclass

from cfte.books.local_book import LocalBook
from cfte.collectors.health import CollectorHealthSnapshot
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedDepthDiff, NormalizedTrade, ThesisSignal
from cfte.thesis.engines import evaluate_setups


@dataclass(slots=True)
class LiveEvaluation:
    event_type: str
    venue_ts: int
    signals: list[ThesisSignal]


class LiveThesisLoop:
    def __init__(self, instrument_key: str, trade_window_size: int = 20) -> None:
        self.instrument_key = instrument_key
        self.trade_window_size = trade_window_size
        self.order_book = LocalBook(instrument_key)
        self.trades: list[NormalizedTrade] = []
        self.previous_cvd = 0.0

    def apply_snapshot(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]], seq_id: int) -> None:
        self.order_book.apply_snapshot(bids=bids, asks=asks, seq_id=seq_id)

    def ingest_depth(self, depth: NormalizedDepthDiff) -> None:
        self.order_book.apply_diff(depth.bid_updates, depth.ask_updates, seq_id=depth.final_update_id)

    def ingest_trade(self, trade: NormalizedTrade) -> LiveEvaluation:
        self.trades.append(trade)
        if len(self.trades) > self.trade_window_size:
            self.trades = self.trades[-self.trade_window_size :]

        snapshot = build_tape_snapshot(
            instrument_key=self.instrument_key,
            order_book=self.order_book,
            trades=self.trades,
            window_start_ts=self.trades[0].venue_ts,
            window_end_ts=trade.venue_ts,
            previous_cvd=self.previous_cvd,
        )
        self.previous_cvd = snapshot.cvd
        return LiveEvaluation(event_type="trade", venue_ts=trade.venue_ts, signals=evaluate_setups(snapshot))


def health_snapshot_to_dict(snapshot: CollectorHealthSnapshot) -> dict[str, object]:
    payload = asdict(snapshot)
    return payload
