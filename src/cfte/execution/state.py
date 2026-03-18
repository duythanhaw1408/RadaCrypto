from __future__ import annotations

from dataclasses import dataclass, field

from cfte.execution.models import CanonicalOrder, FillFact, OrderSnapshot, OrderStatus

_ALLOWED_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    "NEW": {"ACKED", "REJECTED", "CANCELED", "EXPIRED"},
    "ACKED": {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED"},
    "FILLED": set(),
    "CANCELED": set(),
    "REJECTED": set(),
    "EXPIRED": set(),
}


@dataclass(slots=True)
class OrderStateStore:
    _orders: dict[str, OrderSnapshot] = field(default_factory=dict)
    _seen_fill_ids: set[str] = field(default_factory=set)

    def register_order(self, order: CanonicalOrder) -> None:
        if order.order_id in self._orders:
            raise ValueError(f"Order already exists: {order.order_id}")
        self._orders[order.order_id] = OrderSnapshot(order=order, status="NEW", updated_ts=order.created_ts)

    def transition(self, order_id: str, next_status: OrderStatus, updated_ts: int) -> None:
        current = self._get(order_id)
        if next_status == current.status:
            current.updated_ts = updated_ts
            return
        if next_status not in _ALLOWED_TRANSITIONS[current.status]:
            raise ValueError(f"Invalid transition: {current.status} -> {next_status}")
        current.status = next_status
        current.updated_ts = updated_ts

    def apply_fill(self, fill: FillFact) -> None:
        if fill.fill_id in self._seen_fill_ids:
            return

        snapshot = self._get(fill.order_id)
        if snapshot.status in {"REJECTED", "CANCELED", "EXPIRED"}:
            raise ValueError(f"Cannot fill closed order in status: {snapshot.status}")
        if fill.side != snapshot.order.side:
            raise ValueError("Fill side does not match order side")
        if fill.symbol != snapshot.order.symbol:
            raise ValueError("Fill symbol does not match order symbol")

        filled_qty = snapshot.filled_qty + fill.qty
        if filled_qty > snapshot.order.qty + 1e-9:
            raise ValueError("Fill quantity exceeds order quantity")

        total_quote = snapshot.quote_filled + (fill.qty * fill.price)
        snapshot.filled_qty = filled_qty
        snapshot.quote_filled = total_quote
        snapshot.avg_fill_price = total_quote / filled_qty if filled_qty > 0 else None
        snapshot.fee_paid += fill.fee_paid
        snapshot.updated_ts = max(snapshot.updated_ts, fill.venue_ts)
        self._seen_fill_ids.add(fill.fill_id)

        if filled_qty >= snapshot.order.qty - 1e-9:
            snapshot.status = "FILLED"
        else:
            snapshot.status = "PARTIALLY_FILLED"

    def get(self, order_id: str) -> OrderSnapshot:
        return self._get(order_id)

    def all_orders(self) -> list[OrderSnapshot]:
        return list(self._orders.values())

    def _get(self, order_id: str) -> OrderSnapshot:
        if order_id not in self._orders:
            raise KeyError(f"Unknown order_id: {order_id}")
        return self._orders[order_id]
