from pathlib import Path

import pytest

from cfte.replay.adapters import load_replay_events
from cfte.replay.runner import render_replay_summary_vi, run_replay


def test_replay_runner_is_deterministic_for_fixture():
    events_path = Path("fixtures/replay/btcusdt_normalized.jsonl")
    events = load_replay_events(events_path)

    first = run_replay(events)
    second = run_replay(events)

    assert first.fingerprint == second.fingerprint
    assert first.event_count == 6
    assert first.thesis_count == second.thesis_count
    assert [s.thesis_id for s in first.thesis_events] == [s.thesis_id for s in second.thesis_events]


def test_replay_summary_is_vietnamese_facing():
    events = load_replay_events("fixtures/replay/btcusdt_normalized.jsonl")
    result = run_replay(events)
    summary = render_replay_summary_vi(result)

    assert "Replay hoàn tất" in summary
    assert "sự kiện" in summary


def test_run_replay_sorts_events_and_rejects_instrument_mismatch():
    events = load_replay_events("fixtures/replay/btcusdt_normalized.jsonl")
    unordered = list(reversed(events))

    ordered_result = run_replay(events)
    unordered_result = run_replay(unordered)

    assert ordered_result.fingerprint == unordered_result.fingerprint

    bad_trade = unordered[-1]
    bad_trade.payload.instrument_key = "BINANCE:ETHUSDT:SPOT"

    with pytest.raises(ValueError, match="instrument_key mismatch"):
        run_replay(unordered)


from cfte.models.events import NormalizedTrade
from cfte.replay.adapters import ReplayBookSnapshot, ReplayEvent


def test_run_replay_records_lifecycle_history_for_real_stage_progression():
    events = load_replay_events("fixtures/replay/btcusdt_normalized.jsonl")

    result = run_replay(events)

    accumulation = next(state for state in result.thesis_state.values() if state.signal.setup == "stealth_accumulation")
    history = [event for event in result.thesis_event_history if event.thesis_id == accumulation.signal.thesis_id]

    assert [event.to_stage for event in history] == ["DETECTED", "WATCHLIST", "CONFIRMED", "ACTIONABLE", "RESOLVED"]
    assert accumulation.signal.stage == "RESOLVED"
    assert accumulation.closed_ts == 1_700_000_002_000
    assert history[-1].summary_vi.endswith("'Đã hoàn tất'.")


def test_run_replay_reaches_invalidated_terminal_stage_through_real_state_update_path():
    events = [
        ReplayEvent(
            event_type="book_snapshot",
            venue_ts=1_000,
            payload=ReplayBookSnapshot(
                instrument_key="BINANCE:BTCUSDT:SPOT",
                bids=[(100.0, 8.0)],
                asks=[(100.5, 3.0)],
                seq_id=1,
                venue_ts=1_000,
            ),
        ),
        ReplayEvent(
            event_type="trade",
            venue_ts=2_000,
            payload=NormalizedTrade(
                event_id="t1",
                venue="binance",
                instrument_key="BINANCE:BTCUSDT:SPOT",
                price=100.45,
                qty=1.0,
                quote_qty=100.45,
                taker_side="BUY",
                venue_ts=2_000,
            ),
        ),
        ReplayEvent(
            event_type="trade",
            venue_ts=3_000,
            payload=NormalizedTrade(
                event_id="t2",
                venue="binance",
                instrument_key="BINANCE:BTCUSDT:SPOT",
                price=100.48,
                qty=1.2,
                quote_qty=120.576,
                taker_side="BUY",
                venue_ts=3_000,
            ),
        ),
        ReplayEvent(
            event_type="trade",
            venue_ts=4_000,
            payload=NormalizedTrade(
                event_id="t3",
                venue="binance",
                instrument_key="BINANCE:BTCUSDT:SPOT",
                price=99.90,
                qty=2.0,
                quote_qty=199.8,
                taker_side="SELL",
                venue_ts=4_000,
            ),
        ),
    ]

    result = run_replay(events)

    accumulation = next(state for state in result.thesis_state.values() if state.signal.setup == "stealth_accumulation")
    history = [event for event in result.thesis_event_history if event.thesis_id == accumulation.signal.thesis_id]

    assert accumulation.signal.stage == "INVALIDATED"
    assert accumulation.closed_ts == 4_000
    assert [event.to_stage for event in history] == ["DETECTED", "WATCHLIST", "CONFIRMED", "INVALIDATED"]
    assert "Đã bị vô hiệu" in history[-1].summary_vi
