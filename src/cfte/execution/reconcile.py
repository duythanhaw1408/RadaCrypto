from __future__ import annotations

from cfte.execution.models import FillFact, PositionReconciliation


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


def reconcile_position(fills: list[FillFact], symbol: str, venue_net_qty: float, tolerance: float = 1e-6) -> PositionReconciliation:
    internal_qty = internal_net_position_qty(fills, symbol=symbol)
    delta = internal_qty - venue_net_qty
    return PositionReconciliation(
        symbol=symbol,
        internal_net_qty=internal_qty,
        venue_net_qty=venue_net_qty,
        delta_qty=delta,
        is_aligned=abs(delta) <= tolerance,
    )
