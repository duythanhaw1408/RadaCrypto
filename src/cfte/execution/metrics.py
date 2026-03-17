from __future__ import annotations

from cfte.execution.models import ExecutionQualityMetrics, FillFact


def slippage_bps(side: str, decision_price: float, fill_price: float) -> float:
    if decision_price <= 0:
        raise ValueError("decision_price must be > 0")
    if side == "BUY":
        return ((fill_price - decision_price) / decision_price) * 10000
    return ((decision_price - fill_price) / decision_price) * 10000


def markout_bps(side: str, fill_price: float, mark_price_after: float) -> float:
    if fill_price <= 0:
        raise ValueError("fill_price must be > 0")
    if side == "BUY":
        return ((mark_price_after - fill_price) / fill_price) * 10000
    return ((fill_price - mark_price_after) / fill_price) * 10000


def compute_execution_quality(
    fills: list[FillFact],
    decision_price_by_order_id: dict[str, float],
    mark_price_after_by_fill_id: dict[str, float],
) -> ExecutionQualityMetrics:
    if not fills:
        return ExecutionQualityMetrics(fill_count=0, avg_slippage_bps=0.0, avg_markout_bps=0.0)

    slippages: list[float] = []
    markouts: list[float] = []
    for fill in fills:
        decision_price = decision_price_by_order_id.get(fill.order_id)
        if decision_price is not None:
            slippages.append(slippage_bps(fill.side, decision_price, fill.price))

        mark_after = mark_price_after_by_fill_id.get(fill.fill_id)
        if mark_after is not None:
            markouts.append(markout_bps(fill.side, fill.price, mark_after))

    avg_slippage = sum(slippages) / len(slippages) if slippages else 0.0
    avg_markout = sum(markouts) / len(markouts) if markouts else 0.0
    return ExecutionQualityMetrics(fill_count=len(fills), avg_slippage_bps=avg_slippage, avg_markout_bps=avg_markout)
