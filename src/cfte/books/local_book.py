from __future__ import annotations

from dataclasses import dataclass, field

@dataclass
class LocalBook:
    instrument_key: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_seq_id: int | None = None

    def apply_snapshot(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        seq_id: int,
    ) -> None:
        self.bids = {float(px): float(qty) for px, qty in bids if float(qty) > 0}
        self.asks = {float(px): float(qty) for px, qty in asks if float(qty) > 0}
        self.last_seq_id = seq_id

    def apply_diff(
        self,
        bid_updates: list[tuple[float, float]],
        ask_updates: list[tuple[float, float]],
        seq_id: int,
    ) -> None:
        if self.last_seq_id is not None and seq_id < self.last_seq_id:
            return
        for px, qty in bid_updates:
            self._upsert(self.bids, px, qty)
        for px, qty in ask_updates:
            self._upsert(self.asks, px, qty)
        self.last_seq_id = seq_id

    @staticmethod
    def _upsert(side: dict[float, float], px: float, qty: float) -> None:
        px_f = float(px)
        qty_f = float(qty)
        if qty_f <= 0:
            side.pop(px_f, None)
        else:
            side[px_f] = qty_f

    def best_bid(self) -> tuple[float, float]:
        if not self.bids:
            raise ValueError("No bids")
        px = max(self.bids)
        return px, self.bids[px]

    def best_ask(self) -> tuple[float, float]:
        if not self.asks:
            raise ValueError("No asks")
        px = min(self.asks)
        return px, self.asks[px]

    def mid(self) -> float:
        bid_px, _ = self.best_bid()
        ask_px, _ = self.best_ask()
        return (bid_px + ask_px) / 2.0

    def spread_bps(self) -> float:
        bid_px, _ = self.best_bid()
        ask_px, _ = self.best_ask()
        mid = (bid_px + ask_px) / 2.0
        return ((ask_px - bid_px) / mid) * 10000.0

    def imbalance_l1(self) -> float:
        _, bid_qty = self.best_bid()
        _, ask_qty = self.best_ask()
        total = bid_qty + ask_qty
        if total == 0:
            return 0.5
        return bid_qty / total

    def clone(self) -> "LocalBook":
        return LocalBook(
            instrument_key=self.instrument_key,
            bids=dict(self.bids),
            asks=dict(self.asks),
            last_seq_id=self.last_seq_id,
        )
