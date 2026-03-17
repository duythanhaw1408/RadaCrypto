from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade

def test_build_tape_snapshot():
    book = LocalBook("BINANCE:BTCUSDT:SPOT")
    book.apply_snapshot(
        bids=[(100.0, 5.0)],
        asks=[(100.5, 5.0)],
        seq_id=1,
    )
    trades = [
        NormalizedTrade(
            event_id="1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.4,
            qty=1.0,
            quote_qty=100.4,
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
    snap = build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=book,
        trades=trades,
        window_start_ts=1000,
        window_end_ts=2000,
    )
    assert snap.instrument_key == "BINANCE:BTCUSDT:SPOT"
    assert snap.trade_count == 2
    assert snap.spread_bps > 0
