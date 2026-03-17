from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot
from cfte.models.events import NormalizedTrade
from cfte.thesis.engines import assign_stage, evaluate_setups


def _sample_snapshot() -> object:
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
    return build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=book,
        trades=trades,
        window_start_ts=1000,
        window_end_ts=3000,
    )


def test_thesis_engine_emits_required_setups_and_fields():
    signals = evaluate_setups(_sample_snapshot())

    assert [signal.setup for signal in signals] == ["stealth_accumulation", "distribution"]
    for signal in signals:
        assert signal.thesis_id
        assert signal.instrument_key == "BINANCE:BTCUSDT:SPOT"
        assert signal.direction in {"LONG_BIAS", "SHORT_BIAS"}
        assert signal.stage in {"DETECTED", "WATCHLIST", "CONFIRMED", "ACTIONABLE"}
        assert isinstance(signal.score, float)
        assert isinstance(signal.confidence, float)
        assert isinstance(signal.coverage, float)
        assert isinstance(signal.why_now, list)
        assert isinstance(signal.conflicts, list)
        assert signal.invalidation
        assert signal.entry_style
        assert len(signal.targets) >= 2


def test_assign_stage_thresholds():
    assert assign_stage(score=50.0, confidence=0.7) == "DETECTED"
    assert assign_stage(score=65.0, confidence=0.6) == "WATCHLIST"
    assert assign_stage(score=74.0, confidence=0.7) == "CONFIRMED"
    assert assign_stage(score=80.0, confidence=0.7) == "ACTIONABLE"


def test_accumulation_outscores_distribution_in_buy_pressure_snapshot():
    signals = evaluate_setups(_sample_snapshot())

    assert signals[0].setup == "stealth_accumulation"
    assert signals[0].score > signals[1].score
    assert signals[0].confidence >= 0.62
