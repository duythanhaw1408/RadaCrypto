import pytest

from cfte.thesis.lifecycle import (
    ACTIVE_STAGES,
    TERMINAL_STAGES,
    InvalidThesisTransitionError,
    can_transition_stage,
    reduce_thesis_stage,
    summarize_lifecycle_transition,
)


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
