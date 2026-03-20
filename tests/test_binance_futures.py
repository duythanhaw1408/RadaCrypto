from cfte.collectors.binance_futures import (
    BinanceFuturesCollector,
    classify_basis_state,
    summarize_force_orders,
    summarize_futures_agg_trades,
)


def test_summarize_futures_agg_trades_uses_quote_and_taker_direction():
    summary = summarize_futures_agg_trades(
        [
            {"p": "100", "q": "2", "m": False, "T": 1000},
            {"p": "101", "q": "1", "m": True, "T": 1200},
        ]
    )

    assert summary["available"] is True
    assert summary["buy_quote"] == 200.0
    assert summary["sell_quote"] == 101.0
    assert summary["total_quote"] == 301.0
    assert summary["delta_quote"] == 99.0
    assert summary["aggression_ratio"] == 200.0 / 301.0
    assert summary["trade_count"] == 2
    assert summary["last_trade_ts"] == 1200


def test_summarize_force_orders_classifies_liquidation_bias_from_side():
    summary = summarize_force_orders(
        [
            {"o": {"S": "SELL", "ap": "100", "q": "5", "T": 1000}},
            {"o": {"S": "SELL", "ap": "99", "q": "2", "T": 1100}},
            {"o": {"S": "BUY", "ap": "101", "q": "1", "T": 1200}},
        ]
    )

    assert summary["available"] is True
    assert summary["bias"] == "LONGS_FLUSHED"
    assert summary["count"] == 3
    assert summary["quote"] == 799.0
    assert summary["last_liquidation_ts"] == 1200


def test_classify_basis_state_maps_extremes_and_balance():
    assert classify_basis_state(15.0) == "OVERHEATED_PREMIUM"
    assert classify_basis_state(6.0) == "PREMIUM"
    assert classify_basis_state(-6.0) == "DISCOUNT"
    assert classify_basis_state(-15.0) == "DEEP_DISCOUNT"
    assert classify_basis_state(1.0) == "BALANCED"


def test_get_live_context_exposes_phase2_hidden_flow_metrics(monkeypatch):
    collector = BinanceFuturesCollector(symbol="BTCUSDT", context_window_seconds=30.0)
    collector._stream_connected = True
    collector._agg_trade_rows.extend(
        [
            {"ts": 1_000, "p": 100.0, "q": 2.0, "quote_qty": 200.0, "m": False},
            {"ts": 2_000, "p": 101.0, "q": 1.0, "quote_qty": 101.0, "m": True},
        ]
    )
    collector._force_order_rows.extend(
        [
            {"o": {"T": 1_900, "S": "SELL", "ap": 100.0, "q": 5.0, "quote_qty": 500.0}},
        ]
    )
    monkeypatch.setattr(
        collector,
        "_build_rest_context",
        lambda **_: {
            "available": True,
            "fresh": True,
            "timestamp": 2_000,
            "mark_price": 100.0,
            "index_price": 99.9,
            "basis_bps": 10.01,
            "basis_state": "PREMIUM",
            "funding_rate": 0.0001,
            "oi_value": 1_000_000.0,
            "oi_delta": 2_000.0,
            "oi_expansion_ratio": 0.002,
        },
    )

    context = collector.get_live_context(now_ms=2_500)

    assert context["futures_total_quote"] == 301.0
    assert context["futures_aggression_ratio"] == 200.0 / 301.0
    assert context["liquidation_intensity"] == 500.0 / 301.0
    assert context["basis_state"] == "PREMIUM"
    assert context["oi_expansion_ratio"] == 0.002
