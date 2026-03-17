import pytest

from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade


def test_build_tape_snapshot_computes_required_metrics_deterministically():
    book = LocalBook("BINANCE:BTCUSDT:SPOT")
    book.apply_snapshot(
        bids=[(100.0, 6.0)],
        asks=[(100.5, 4.0)],
        seq_id=1,
    )
    trades = [
        NormalizedTrade(
            event_id="1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.40,
            qty=1.0,
            quote_qty=100.40,
            taker_side="BUY",
            venue_ts=1000,
        ),
        NormalizedTrade(
            event_id="2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.45,
            qty=1.0,
            quote_qty=100.45,
            taker_side="SELL",
            venue_ts=2000,
        ),
    ]

    snapshot = build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=book,
        trades=trades,
        window_start_ts=1000,
        window_end_ts=2000,
        previous_cvd=1000.0,
    )

    assert round(snapshot.spread_bps, 2) == 49.88
    assert snapshot.microprice == pytest.approx(100.3)
    assert snapshot.imbalance_l1 == pytest.approx(0.6)
    assert snapshot.delta_quote == pytest.approx(-0.05)
    assert snapshot.cvd == pytest.approx(999.95)
    assert snapshot.trade_burst == pytest.approx(2.0)
    assert snapshot.absorption_proxy == pytest.approx(10.06760625)
