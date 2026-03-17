import pytest

from cfte.features.venue_compare import compare_trade_flows, render_venue_comparison_vi
from cfte.models.events import NormalizedTrade


def _trade(venue: str, quote_qty: float, price: float = 40000.0) -> NormalizedTrade:
    qty = quote_qty / price
    return NormalizedTrade(
        event_id=f"{venue}-{quote_qty}",
        venue=venue,
        instrument_key=f"{venue.upper()}:BTCUSDT:PERP",
        price=price,
        qty=qty,
        quote_qty=quote_qty,
        taker_side="BUY",
        venue_ts=1700000001000,
    )


def test_compare_trade_flows_builds_leader_lagger_foundation():
    trades = [
        _trade("binance", 10000),
        _trade("binance", 9000),
        _trade("bybit", 6000, price=40010),
        _trade("okx", 2000, price=39990),
    ]

    result = compare_trade_flows(trades)

    assert result.symbol == "BTCUSDT"
    assert result.leader_venue == "binance"
    assert result.lagger_venue == "okx"
    assert result.vwap_spread_bps > 0


def test_render_venue_comparison_vi_is_vietnamese_user_facing():
    result = compare_trade_flows([_trade("binance", 10000), _trade("bybit", 8000, price=40020), _trade("okx", 7000, price=40010)])
    summary = render_venue_comparison_vi(result)

    assert "So sánh đa sàn" in summary
    assert "Sàn dẫn nhịp" in summary
    assert "Độ lệch VWAP" in summary


def test_compare_trade_flows_requires_two_venues():
    with pytest.raises(ValueError):
        compare_trade_flows([_trade("binance", 1000)])
