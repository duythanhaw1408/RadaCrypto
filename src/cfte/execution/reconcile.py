from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from cfte.execution.models import FillFact, PositionReconciliation


@dataclass(frozen=True, slots=True)
class ReconciliationFillView:
    ordered_unique_fills: list[FillFact]
    duplicate_fill_ids: tuple[str, ...]
    out_of_order_fill_count: int


def _ordered_unique_fills(fills: list[FillFact]) -> ReconciliationFillView:
    seen_fill_ids: set[str] = set()
    unique_in_arrival_order: list[FillFact] = []
    duplicate_fill_ids: list[str] = []

    for fill in fills:
        if fill.fill_id in seen_fill_ids:
            duplicate_fill_ids.append(fill.fill_id)
            continue
        seen_fill_ids.add(fill.fill_id)
        unique_in_arrival_order.append(fill)

    ordered_unique_fills = sorted(unique_in_arrival_order, key=lambda fill: (fill.venue_ts, fill.fill_id))
    out_of_order_fill_count = sum(
        1
        for arrival_index, ordered_fill in enumerate(ordered_unique_fills)
        if unique_in_arrival_order[arrival_index].fill_id != ordered_fill.fill_id
    )
    return ReconciliationFillView(
        ordered_unique_fills=ordered_unique_fills,
        duplicate_fill_ids=tuple(duplicate_fill_ids),
        out_of_order_fill_count=out_of_order_fill_count,
    )


def internal_net_position_qty(fills: list[FillFact], symbol: str) -> float:
    net = 0.0
    for fill in fills:
        if fill.symbol != symbol:
            continue
        if fill.side == "BUY":
            net += fill.qty
        else:
            net -= fill.qty
    return net


def reconcile_position(
    fills: list[FillFact],
    symbol: str,
    venue_net_qty: float,
    tolerance: float = 1e-6,
    order_qty_by_order_id: dict[str, float] | None = None,
) -> PositionReconciliation:
    fill_view = _ordered_unique_fills(fills)
    ordered_unique_fills = fill_view.ordered_unique_fills

    gross_buy_qty = 0.0
    gross_sell_qty = 0.0
    per_order_qty: dict[str, float] = defaultdict(float)
    qty_violations: list[str] = []

    for fill in ordered_unique_fills:
        if fill.symbol != symbol:
            continue
        if fill.side == "BUY":
            gross_buy_qty += fill.qty
        else:
            gross_sell_qty += fill.qty

        per_order_qty[fill.order_id] += fill.qty
        order_qty = None if order_qty_by_order_id is None else order_qty_by_order_id.get(fill.order_id)
        if order_qty is not None and per_order_qty[fill.order_id] > order_qty + tolerance:
            qty_violations.append(fill.order_id)

    internal_qty = gross_buy_qty - gross_sell_qty
    delta = internal_qty - venue_net_qty
    has_structural_issues = bool(fill_view.duplicate_fill_ids or qty_violations)
    return PositionReconciliation(
        symbol=symbol,
        internal_net_qty=internal_qty,
        venue_net_qty=venue_net_qty,
        delta_qty=delta,
        gross_buy_qty=gross_buy_qty,
        gross_sell_qty=gross_sell_qty,
        unique_fill_count=len(ordered_unique_fills),
        duplicate_fill_count=len(fill_view.duplicate_fill_ids),
        out_of_order_fill_count=fill_view.out_of_order_fill_count,
        qty_violation_count=len(set(qty_violations)),
        has_structural_issues=has_structural_issues,
        is_aligned=abs(delta) <= tolerance and not has_structural_issues,
    )
