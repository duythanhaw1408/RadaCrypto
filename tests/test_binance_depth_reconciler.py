from cfte.books.binance_depth import BinanceDepthReconciler
from cfte.models.events import NormalizedDepthDiff


INSTRUMENT = "BINANCE:BTCUSDT:SPOT"


def _diff(first_id: int, final_id: int, bid_updates=None, ask_updates=None) -> NormalizedDepthDiff:
    return NormalizedDepthDiff(
        event_id=f"d-{first_id}-{final_id}",
        venue="binance",
        instrument_key=INSTRUMENT,
        first_update_id=first_id,
        final_update_id=final_id,
        bid_updates=bid_updates or [(100.0, 2.0)],
        ask_updates=ask_updates or [(100.5, 3.0)],
        venue_ts=1700000000000,
    )


def test_reconcile_with_buffered_diff_after_snapshot():
    reconciler = BinanceDepthReconciler(instrument_key=INSTRUMENT)

    assert reconciler.ingest_diff(_diff(101, 105)) is False
    assert reconciler.is_synced is False

    reconciler.apply_snapshot(
        bids=[(100.0, 1.0)],
        asks=[(100.5, 1.0)],
        last_update_id=100,
    )

    assert reconciler.is_synced is True
    assert reconciler.book.last_seq_id == 105
    assert reconciler.book.best_bid() == (100.0, 2.0)


def test_detect_gap_and_mark_unsynced():
    reconciler = BinanceDepthReconciler(instrument_key=INSTRUMENT)
    reconciler.apply_snapshot(
        bids=[(100.0, 1.0)],
        asks=[(100.5, 1.0)],
        last_update_id=100,
    )

    applied = reconciler.ingest_diff(_diff(150, 155))

    assert applied is False
    assert reconciler.is_synced is False
    assert reconciler.book.last_seq_id == 100
