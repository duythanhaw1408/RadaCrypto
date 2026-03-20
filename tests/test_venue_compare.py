import pytest

from cfte.features.venue_compare import (
    build_venue_confirmation_context,
    compare_trade_flows,
    render_venue_comparison_vi,
)
from cfte.models.events import NormalizedTrade


BASE_TS = 1700000001000


def _trade(venue: str, quote_qty: float, price: float = 40000.0, venue_ts: int = BASE_TS) -> NormalizedTrade:
    qty = quote_qty / price
    return NormalizedTrade(
        event_id=f"{venue}-{quote_qty}-{venue_ts}",
        venue=venue,
        instrument_key=f"{venue.upper()}:BTCUSDT:PERP",
        price=price,
        qty=qty,
        quote_qty=quote_qty,
        taker_side="BUY",
        venue_ts=venue_ts,
    )


def test_compare_trade_flows_builds_leader_lagger_foundation():
    trades = [
        _trade("binance", 10000, venue_ts=BASE_TS),
        _trade("binance", 9000, venue_ts=BASE_TS + 500),
        _trade("bybit", 6000, price=40010, venue_ts=BASE_TS + 200),
        _trade("okx", 2000, price=39990, venue_ts=BASE_TS + 400),
    ]

    result = compare_trade_flows(trades)

    assert result.symbol == "BTCUSDT"
    assert result.leader_venue == "binance"
    assert result.lagger_venue == "okx"
    assert result.vwap_spread_bps > 0
    assert result.aligned_window_ms > 0
    assert result.leader_confidence > 0
    assert result.discovery_phase != "UNKNOWN"
    assert result.excluded_venues == []



def test_render_venue_comparison_vi_is_vietnamese_user_facing():
    result = compare_trade_flows(
        [
            _trade("binance", 10000, venue_ts=BASE_TS),
            _trade("bybit", 8000, price=40020, venue_ts=BASE_TS + 300),
            _trade("okx", 7000, price=40010, venue_ts=BASE_TS + 500),
        ]
    )
    summary = render_venue_comparison_vi(result)

    assert "So sánh đa sàn" in summary
    assert "Sàn dẫn nhịp" in summary
    assert "Độ lệch VWAP" in summary
    assert "Cửa sổ so sánh hợp lệ" in summary
    assert "Độ tin cậy leader" in summary


def test_render_venue_comparison_vi_translates_excluded_reason_labels():
    result = compare_trade_flows(
        [
            _trade("binance", 12000, venue_ts=BASE_TS + 4000),
            _trade("binance", 10000, venue_ts=BASE_TS + 4500),
            _trade("bybit", 9000, venue_ts=BASE_TS + 4300),
            _trade("okx", 7000, venue_ts=BASE_TS),
        ]
    )
    summary = render_venue_comparison_vi(result)

    assert "Dữ liệu cửa sổ đã cũ so với sàn tham chiếu" in summary
    assert "stale_window" not in summary


def test_compare_trade_flows_requires_two_venues():
    with pytest.raises(ValueError):
        compare_trade_flows([_trade("binance", 1000)])



def test_compare_trade_flows_computes_vwap_from_qty_not_quote_weighted_price():
    trades = [
        NormalizedTrade(
            event_id="binance-1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:PERP",
            price=100.0,
            qty=1.0,
            quote_qty=100.0,
            taker_side="BUY",
            venue_ts=BASE_TS,
        ),
        NormalizedTrade(
            event_id="binance-2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:PERP",
            price=200.0,
            qty=1.0,
            quote_qty=200.0,
            taker_side="BUY",
            venue_ts=BASE_TS + 100,
        ),
        _trade("bybit", 5000, price=150.0, venue_ts=BASE_TS + 100),
    ]

    result = compare_trade_flows(trades)
    binance_flow = next(flow for flow in result.flows if flow.venue == "binance")

    assert binance_flow.vwap == pytest.approx(150.0)



def test_compare_trade_flows_excludes_stale_venue_windows_deterministically():
    trades = [
        _trade("binance", 12000, venue_ts=BASE_TS + 4000),
        _trade("binance", 10000, venue_ts=BASE_TS + 4500),
        _trade("bybit", 9000, venue_ts=BASE_TS + 4300),
        _trade("okx", 7000, venue_ts=BASE_TS),
    ]

    result = compare_trade_flows(trades)

    assert result.included_venues == ["binance", "bybit"]
    assert [(status.venue, status.reason) for status in result.excluded_venues] == [
        ("okx", "stale_window")
    ]



def test_compare_trade_flows_rejects_non_overlapping_venue_windows():
    trades = [
        _trade("binance", 12000, venue_ts=BASE_TS),
        _trade("binance", 10000, venue_ts=BASE_TS + 500),
        _trade("bybit", 9000, venue_ts=BASE_TS + 4000),
        _trade("bybit", 7000, venue_ts=BASE_TS + 4500),
    ]

    with pytest.raises(ValueError, match="at least two aligned fresh venues"):
        compare_trade_flows(trades)



def test_compare_trade_flows_rejects_when_misalignment_leaves_only_one_usable_venue():
    trades = [
        _trade("binance", 12000, venue_ts=BASE_TS),
        _trade("binance", 10000, venue_ts=BASE_TS + 100),
        _trade("bybit", 9000, venue_ts=BASE_TS),
        _trade("bybit", 8000, venue_ts=BASE_TS + 100),
        _trade("okx", 7000, venue_ts=BASE_TS + 3000),
        _trade("okx", 6000, venue_ts=BASE_TS + 3100),
    ]

    with pytest.raises(ValueError, match="at least two aligned fresh venues"):
        compare_trade_flows(trades)


def test_build_venue_confirmation_context_marks_binance_lead_as_confirmed():
    result = compare_trade_flows(
        [
            _trade("binance", 12000, venue_ts=BASE_TS),
            _trade("binance", 10000, venue_ts=BASE_TS + 100),
            _trade("bybit", 8000, price=40005, venue_ts=BASE_TS + 120),
            _trade("okx", 7000, price=40003, venue_ts=BASE_TS + 150),
        ]
    )

    context = build_venue_confirmation_context(result, primary_venue="binance", max_confirmed_vwap_spread_bps=5.0)

    assert context["venue_confirmation_state"] == "CONFIRMED"
    assert context["leader_venue"] == "binance"
    assert context["leader_confidence"] > 0.45
    assert context["aligned_window_ms"] > 0


def test_build_venue_confirmation_context_marks_alt_lead_when_primary_lags():
    result = compare_trade_flows(
        [
            _trade("binance", 6000, venue_ts=BASE_TS),
            _trade("bybit", 12000, price=40004, venue_ts=BASE_TS + 50),
            _trade("okx", 7000, price=40002, venue_ts=BASE_TS + 90),
        ]
    )

    context = build_venue_confirmation_context(result, primary_venue="binance", max_confirmed_vwap_spread_bps=5.0)

    assert context["venue_confirmation_state"] == "ALT_LEAD"
    assert context["leader_venue"] == "bybit"


def test_build_venue_confirmation_context_marks_large_vwap_spread_as_divergent():
    result = compare_trade_flows(
        [
            _trade("binance", 12000, price=40000, venue_ts=BASE_TS),
            _trade("bybit", 8000, price=40150, venue_ts=BASE_TS + 50),
            _trade("okx", 7000, price=39900, venue_ts=BASE_TS + 90),
        ]
    )

    context = build_venue_confirmation_context(result, primary_venue="binance", max_confirmed_vwap_spread_bps=5.0)

    assert context["venue_confirmation_state"] == "DIVERGENT"
