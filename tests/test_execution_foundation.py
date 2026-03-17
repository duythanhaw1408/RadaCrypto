import pytest

from cfte.execution.ledger import FillLedger
from cfte.execution.metrics import compute_execution_quality, markout_bps, slippage_bps
from cfte.execution.models import CanonicalOrder, FillFact
from cfte.execution.reconcile import reconcile_position
from cfte.execution.state import OrderStateStore
from cfte.execution.summary import build_execution_summary, render_execution_summary_vi


def _sample_order(order_id: str = "ord-1", qty: float = 2.0) -> CanonicalOrder:
    return CanonicalOrder(
        order_id=order_id,
        client_order_id=f"client-{order_id}",
        venue="binance",
        account_id="acct-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        qty=qty,
        price=100.0,
        created_ts=1,
    )


def _fill(fill_id: str, order_id: str, qty: float, price: float, ts: int = 2) -> FillFact:
    return FillFact(
        fill_id=fill_id,
        order_id=order_id,
        venue="binance",
        account_id="acct-1",
        symbol="BTCUSDT",
        side="BUY",
        qty=qty,
        price=price,
        fee_paid=0.01,
        fee_asset="USDT",
        liquidity="TAKER",
        venue_ts=ts,
    )


def test_order_lifecycle_state_handling():
    store = OrderStateStore()
    order = _sample_order()
    store.register_order(order)

    store.transition(order.order_id, "ACKED", updated_ts=2)
    store.apply_fill(_fill("fill-1", order.order_id, qty=1.0, price=101.0, ts=3))
    snapshot = store.get(order.order_id)
    assert snapshot.status == "PARTIALLY_FILLED"
    assert snapshot.filled_qty == 1.0

    store.apply_fill(_fill("fill-2", order.order_id, qty=1.0, price=99.0, ts=4))
    snapshot = store.get(order.order_id)
    assert snapshot.status == "FILLED"
    assert snapshot.avg_fill_price == pytest.approx(100.0)

    with pytest.raises(ValueError):
        store.transition(order.order_id, "ACKED", updated_ts=5)


def test_fill_ingestion_append_only():
    ledger = FillLedger()
    first = _fill("fill-1", "ord-1", qty=0.5, price=100.0)
    second = _fill("fill-2", "ord-1", qty=0.3, price=100.5)

    ledger.append_fill(first)
    ledger.append_fill(second)

    assert len(ledger.all_fills()) == 2
    assert len(ledger.fills_for_order("ord-1")) == 2

    with pytest.raises(ValueError):
        ledger.append_fill(first)


def test_reconciliation_logic():
    fills = [
        FillFact("f1", "o1", "binance", "acct-1", "BTCUSDT", "BUY", 1.0, 100.0, 0.0, "USDT", "TAKER", 1),
        FillFact("f2", "o2", "binance", "acct-1", "BTCUSDT", "SELL", 0.4, 102.0, 0.0, "USDT", "MAKER", 2),
    ]

    result = reconcile_position(fills=fills, symbol="BTCUSDT", venue_net_qty=0.59, tolerance=0.02)

    assert result.internal_net_qty == pytest.approx(0.6)
    assert result.delta_qty == pytest.approx(0.01)
    assert result.is_aligned


def test_slippage_metric_computation():
    buy_slippage = slippage_bps(side="BUY", decision_price=100.0, fill_price=100.5)
    sell_slippage = slippage_bps(side="SELL", decision_price=100.0, fill_price=99.5)
    buy_markout = markout_bps(side="BUY", fill_price=100.0, mark_price_after=101.0)

    assert buy_slippage == pytest.approx(50.0)
    assert sell_slippage == pytest.approx(50.0)
    assert buy_markout == pytest.approx(100.0)


def test_execution_summary_defaults_to_vietnamese_text():
    store = OrderStateStore()
    order = _sample_order(qty=1.0)
    store.register_order(order)
    fill = _fill("fill-1", order.order_id, qty=1.0, price=100.2, ts=3)
    store.apply_fill(fill)

    reconciliation = reconcile_position([fill], symbol="BTCUSDT", venue_net_qty=1.0)
    quality = compute_execution_quality(
        fills=[fill],
        decision_price_by_order_id={order.order_id: 100.0},
        mark_price_after_by_fill_id={fill.fill_id: 100.4},
    )
    summary = build_execution_summary(store.all_orders(), [fill], reconciliation, quality)
    text = render_execution_summary_vi(summary)

    assert "Tóm tắt giám sát thực thi" in text
    assert "Đối soát vị thế" in text
