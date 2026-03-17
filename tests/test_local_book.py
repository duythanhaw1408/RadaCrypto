from cfte.books.local_book import LocalBook

def test_local_book_snapshot_and_diff():
    book = LocalBook("BINANCE:BTCUSDT:SPOT")
    book.apply_snapshot(
        bids=[(100.0, 5.0), (99.5, 2.0)],
        asks=[(100.5, 4.0), (101.0, 3.0)],
        seq_id=10,
    )
    assert book.best_bid() == (100.0, 5.0)
    assert book.best_ask() == (100.5, 4.0)

    book.apply_diff(
        bid_updates=[(100.0, 6.0), (99.0, 1.0)],
        ask_updates=[(100.5, 0.0), (100.6, 2.5)],
        seq_id=11,
    )
    assert book.best_bid() == (100.0, 6.0)
    assert book.best_ask() == (100.6, 2.5)
