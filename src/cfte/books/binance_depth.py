from __future__ import annotations

from dataclasses import dataclass, field

from cfte.books.local_book import LocalBook
from cfte.models.events import NormalizedDepthDiff


@dataclass(slots=True)
class BinanceDepthReconciler:
    instrument_key: str
    book: LocalBook = field(init=False)
    _buffer: list[NormalizedDepthDiff] = field(default_factory=list)
    _is_synced: bool = False

    def __post_init__(self) -> None:
        self.book = LocalBook(self.instrument_key)

    @property
    def is_synced(self) -> bool:
        return self._is_synced

    def ingest_diff(self, diff: NormalizedDepthDiff) -> bool:
        if not self._is_synced:
            self._buffer.append(diff)
            return False
        return self._apply_diff(diff)

    def apply_snapshot(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        last_update_id: int,
    ) -> None:
        self.book.apply_snapshot(bids=bids, asks=asks, seq_id=last_update_id)
        self._is_synced = True

        retained = [d for d in self._buffer if d.final_update_id > last_update_id]
        retained.sort(key=lambda d: d.final_update_id)
        self._buffer.clear()

        for diff in retained:
            if diff.first_update_id <= (self.book.last_seq_id or 0) + 1 <= diff.final_update_id:
                self._apply_diff(diff)

    def _apply_diff(self, diff: NormalizedDepthDiff) -> bool:
        last_seq = self.book.last_seq_id
        if last_seq is None:
            return False
        if diff.final_update_id <= last_seq:
            return False
        if diff.first_update_id > last_seq + 1:
            self._is_synced = False
            self._buffer = [diff]
            return False

        self.book.apply_diff(
            bid_updates=diff.bid_updates,
            ask_updates=diff.ask_updates,
            seq_id=diff.final_update_id,
        )
        return True
