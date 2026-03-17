from pathlib import Path

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
