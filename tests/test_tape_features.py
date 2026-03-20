import pytest

from cfte.books.local_book import LocalBook
from cfte.features.tape import build_tape_snapshot, recent_quote_share, slice_trade_window
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
    assert snapshot.metadata["recent_quote_share"] == pytest.approx(1.0)
    assert snapshot.metadata["window_trade_count"] == 2
    assert snapshot.metadata["aggression_ratio"] == pytest.approx(100.40 / (100.40 + 100.45))
    assert snapshot.metadata["sweep_buy_quote"] == 0.0
    assert snapshot.metadata["sweep_sell_quote"] == 0.0


def test_build_tape_snapshot_exposes_directional_sweep_and_replenishment():
    before_book = LocalBook("BINANCE:BTCUSDT:SPOT")
    before_book.apply_snapshot(
        bids=[(100.0, 2.0)],
        asks=[(100.5, 2.0)],
        seq_id=1,
    )
    after_book = LocalBook("BINANCE:BTCUSDT:SPOT")
    after_book.apply_snapshot(
        bids=[(100.0, 4.0)],
        asks=[(100.5, 1.0)],
        seq_id=2,
    )
    trades = [
        NormalizedTrade(
            event_id="s1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.45,
            qty=1.0,
            quote_qty=100.45,
            taker_side="BUY",
            venue_ts=1_000,
        ),
        NormalizedTrade(
            event_id="s2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.50,
            qty=1.0,
            quote_qty=100.50,
            taker_side="BUY",
            venue_ts=1_000,
        ),
        NormalizedTrade(
            event_id="s3",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.0,
            qty=3.0,
            quote_qty=300.0,
            taker_side="SELL",
            venue_ts=2_000,
        ),
    ]

    snapshot = build_tape_snapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        order_book=after_book,
        trades=trades,
        window_start_ts=1_000,
        window_end_ts=5_000,
        previous_cvd=0.0,
        before_book=before_book,
    )

    assert snapshot.metadata["sweep_buy_quote"] == pytest.approx(200.95)
    assert snapshot.metadata["sweep_sell_quote"] == pytest.approx(0.0)
    assert snapshot.metadata["replenishment_bid_score"] > 0.0
    assert snapshot.metadata["replenishment_bid_score"] > snapshot.metadata["replenishment_ask_score"]
    assert 0.0 <= snapshot.metadata["burst_persistence"] <= 1.0
    assert snapshot.metadata["microprice_drift_bps"] != 0.0


def test_slice_trade_window_keeps_recent_trades_and_applies_cap():
    trades = [
        NormalizedTrade(
            event_id=str(index),
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.0 + index,
            qty=1.0,
            quote_qty=100.0 + index,
            taker_side="BUY",
            venue_ts=ts,
        )
        for index, ts in enumerate([1_000, 30_000, 50_000, 61_000], start=1)
    ]

    windowed = slice_trade_window(trades, end_ts=61_000, lookback_seconds=20.0, max_trades=2)

    assert [trade.event_id for trade in windowed] == ["3", "4"]


def test_recent_quote_share_tracks_fresh_flow_share():
    trades = [
        NormalizedTrade(
            event_id="old-1",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.0,
            qty=1.0,
            quote_qty=100.0,
            taker_side="BUY",
            venue_ts=1_000,
        ),
        NormalizedTrade(
            event_id="old-2",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.0,
            qty=1.0,
            quote_qty=100.0,
            taker_side="BUY",
            venue_ts=10_000,
        ),
        NormalizedTrade(
            event_id="recent",
            venue="binance",
            instrument_key="BINANCE:BTCUSDT:SPOT",
            price=100.0,
            qty=0.5,
            quote_qty=50.0,
            taker_side="BUY",
            venue_ts=29_000,
        ),
    ]

    share = recent_quote_share(trades, end_ts=30_000, duration_seconds=10.0)

    assert share == pytest.approx(0.2)
