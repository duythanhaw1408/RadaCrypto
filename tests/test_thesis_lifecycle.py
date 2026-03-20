import pytest

from cfte.thesis.lifecycle import (
    ACTIVE_STAGES,
    TERMINAL_STAGES,
    InvalidThesisTransitionError,
    can_transition_stage,
    reduce_thesis_stage,
    summarize_lifecycle_transition,
)
from cfte.models.events import ThesisSignal
from cfte.thesis.state import ThesisLifecycleRecord, apply_signal_update


@pytest.mark.parametrize(
    ("current_stage", "next_stage"),
    [
        ("DETECTED", "WATCHLIST"),
        ("WATCHLIST", "CONFIRMED"),
        ("CONFIRMED", "ACTIONABLE"),
        ("DETECTED", "INVALIDATED"),
        ("WATCHLIST", "INVALIDATED"),
        ("CONFIRMED", "INVALIDATED"),
        ("ACTIONABLE", "INVALIDATED"),
        ("DETECTED", "RESOLVED"),
        ("WATCHLIST", "RESOLVED"),
        ("CONFIRMED", "RESOLVED"),
        ("ACTIONABLE", "RESOLVED"),
        ("ACTIONABLE", "ACTIONABLE"),
        ("INVALIDATED", "INVALIDATED"),
        ("RESOLVED", "RESOLVED"),
    ],
)
def test_reduce_thesis_stage_accepts_valid_progression_and_terminal_transitions(current_stage: str, next_stage: str):
    assert can_transition_stage(current_stage=current_stage, next_stage=next_stage) is True
    assert reduce_thesis_stage(current_stage=current_stage, next_stage=next_stage) == next_stage


@pytest.mark.parametrize(
    ("current_stage", "next_stage"),
    [
        ("DETECTED", "CONFIRMED"),
        ("DETECTED", "ACTIONABLE"),
        ("WATCHLIST", "ACTIONABLE"),
        ("WATCHLIST", "DETECTED"),
        ("CONFIRMED", "WATCHLIST"),
        ("ACTIONABLE", "CONFIRMED"),
        ("INVALIDATED", "DETECTED"),
        ("INVALIDATED", "RESOLVED"),
        ("RESOLVED", "WATCHLIST"),
        ("RESOLVED", "INVALIDATED"),
    ],
)
def test_reduce_thesis_stage_rejects_invalid_transitions(current_stage: str, next_stage: str):
    assert can_transition_stage(current_stage=current_stage, next_stage=next_stage) is False

    with pytest.raises(InvalidThesisTransitionError, match=f"{current_stage} -> {next_stage}"):
        reduce_thesis_stage(current_stage=current_stage, next_stage=next_stage)


@pytest.mark.parametrize("terminal_stage", TERMINAL_STAGES)
def test_terminal_stages_are_reachable_from_every_active_stage(terminal_stage: str):
    for active_stage in ACTIVE_STAGES:
        assert reduce_thesis_stage(current_stage=active_stage, next_stage=terminal_stage) == terminal_stage


def test_summarize_lifecycle_transition_defaults_to_vietnamese() -> None:
    summary = summarize_lifecycle_transition(current_stage="CONFIRMED", next_stage="INVALIDATED")

    assert summary == "Luận điểm chuyển trạng thái từ 'Đã xác nhận' sang 'Đã bị vô hiệu'."


def test_apply_signal_update_uses_opened_event_for_new_signal():
    signal = ThesisSignal(
        thesis_id="abc",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="stealth_accumulation",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=82.0,
        confidence=0.88,
        coverage=0.8,
        why_now=["Delta mua dương"],
        conflicts=[],
        invalidation="Mất bid hỗ trợ dưới 70000.0",
        entry_style="Canh hồi về bid",
        targets=["TP1", "TP2"],
    )

    _, events = apply_signal_update(None, signal, event_ts=1234)

    assert len(events) == 1
    assert events[0].event_type == "opened"
    assert events[0].summary_vi == "Khởi tạo luận điểm ở trạng thái 'Có thể hành động'."


def test_apply_signal_update_keeps_terminal_signal_snapshot_frozen():
    terminal_signal = ThesisSignal(
        thesis_id="abc",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="distribution",
        direction="SHORT_BIAS",
        stage="INVALIDATED",
        score=38.0,
        confidence=0.76,
        coverage=0.8,
        why_now=["Lực bán phản công sau breakout: -85.65"],
        conflicts=["Giá chưa thất bại rõ khỏi vùng breakout"],
        invalidation="Giá quay lại trên ask 70204.14 và giữ được",
        entry_style="Ưu tiên vào khi retest thất bại vùng breakout cũ",
        targets=["TP1", "TP2"],
    )
    state = ThesisLifecycleRecord(signal=terminal_signal, opened_ts=1000, updated_ts=1100, closed_ts=1200)

    fresh_signal = ThesisSignal(
        thesis_id="abc",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="distribution",
        direction="SHORT_BIAS",
        stage="DETECTED",
        score=99.0,
        confidence=0.95,
        coverage=0.8,
        why_now=["Delta mua dương: 17326.38"],
        conflicts=[],
        invalidation="Giá reclaim lên trên ask 70204.14",
        entry_style="Canh failed reclaim và lower-high",
        targets=["TP1", "TP2"],
    )

    next_state, events = apply_signal_update(state, fresh_signal, event_ts=1300)

    assert events == []
    assert next_state.signal.stage == "INVALIDATED"
    assert next_state.signal.score == 38.0
    assert next_state.signal.why_now == ["Lực bán phản công sau breakout: -85.65"]
