from __future__ import annotations

from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade
from cfte.thesis.engines import evaluate_setups
from cfte.thesis.cards import render_trader_card

def main() -> None:
    book = LocalBook("BINANCE:BTCUSDT:SPOT")
    book.apply_snapshot(
        bids=[(100.0, 5.0), (99.5, 4.0)],
        asks=[(100.5, 6.0), (101.0, 3.5)],
        seq_id=1,
    )

    trades = [
        NormalizedTrade(
            event_id="t1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.4,
            qty=1.5,
            quote_qty=150.6,
            taker_side="BUY",
            venue_ts=1700000001000,
        ),
        NormalizedTrade(
            event_id="t2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.45,
            qty=1.2,
            quote_qty=120.54,
            taker_side="BUY",
            venue_ts=1700000001500,
        ),
        NormalizedTrade(
            event_id="t3",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.48,
            qty=0.8,
            quote_qty=80.384,
            taker_side="BUY",
            venue_ts=1700000002000,
        ),
    ]

    snap = build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=book,
        trades=trades,
        window_start_ts=1700000001000,
        window_end_ts=1700000002000,
    )
    signals = evaluate_setups(snap)
    for signal in signals:
        print(render_trader_card(signal))

if __name__ == "__main__":
    main()
