from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade
from cfte.thesis.engines import evaluate_setups

def test_thesis_engine_emits_signals():
    book = LocalBook("BINANCE:BTCUSDT:SPOT")
    book.apply_snapshot(
        bids=[(100.0, 8.0)],
        asks=[(100.5, 3.0)],
        seq_id=1,
    )
    trades = [
        NormalizedTrade(
            event_id="1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.45,
            qty=1.0,
            quote_qty=100.45,
            taker_side="BUY",
            venue_ts=1000,
        ),
        NormalizedTrade(
            event_id="2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.48,
            qty=1.2,
            quote_qty=120.576,
            taker_side="BUY",
            venue_ts=2000,
        ),
        NormalizedTrade(
            event_id="3",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.49,
            qty=1.3,
            quote_qty=130.637,
            taker_side="BUY",
            venue_ts=3000,
        ),
    ]
    snap = build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=book,
        trades=trades,
        window_start_ts=1000,
        window_end_ts=3000,
    )
    signals = evaluate_setups(snap)
    assert len(signals) >= 2
    assert signals[0].setup in {"stealth_accumulation", "distribution"}
