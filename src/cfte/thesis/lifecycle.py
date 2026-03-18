from __future__ import annotations

from typing import Final

from cfte.models.events import Stage

ACTIVE_STAGES: Final[tuple[Stage, ...]] = (
    "DETECTED",
    "WATCHLIST",
    "CONFIRMED",
    "ACTIONABLE",
)
TERMINAL_STAGES: Final[tuple[Stage, ...]] = ("INVALIDATED", "RESOLVED")

_STAGE_ORDER: Final[dict[Stage, int]] = {
    "DETECTED": 0,
    "WATCHLIST": 1,
    "CONFIRMED": 2,
    "ACTIONABLE": 3,
    "INVALIDATED": 4,
    "RESOLVED": 5,
}

_STAGE_LABELS_VI: Final[dict[Stage, str]] = {
    "DETECTED": "Mới phát hiện",
    "WATCHLIST": "Đưa vào danh sách theo dõi",
    "CONFIRMED": "Đã xác nhận",
    "ACTIONABLE": "Có thể hành động",
    "INVALIDATED": "Đã bị vô hiệu",
    "RESOLVED": "Đã hoàn tất",
}


def stage_label_vi(stage: Stage) -> str:
    return _STAGE_LABELS_VI[stage]


class InvalidThesisTransitionError(ValueError):
    """Raised when a thesis lifecycle transition violates deterministic guardrails."""


_ALLOWED_TRANSITIONS: Final[dict[Stage, frozenset[Stage]]] = {
    "DETECTED": frozenset({"DETECTED", "WATCHLIST", "INVALIDATED", "RESOLVED"}),
    "WATCHLIST": frozenset({"WATCHLIST", "CONFIRMED", "INVALIDATED", "RESOLVED"}),
    "CONFIRMED": frozenset({"CONFIRMED", "ACTIONABLE", "INVALIDATED", "RESOLVED"}),
    "ACTIONABLE": frozenset({"ACTIONABLE", "INVALIDATED", "RESOLVED"}),
    "INVALIDATED": frozenset({"INVALIDATED"}),
    "RESOLVED": frozenset({"RESOLVED"}),
}


def can_transition_stage(current_stage: Stage, next_stage: Stage) -> bool:
    return next_stage in _ALLOWED_TRANSITIONS[current_stage]


def reduce_thesis_stage(current_stage: Stage, next_stage: Stage) -> Stage:
    if can_transition_stage(current_stage=current_stage, next_stage=next_stage):
        return next_stage

    raise InvalidThesisTransitionError(
        "Invalid thesis stage transition: "
        f"{current_stage} -> {next_stage}. "
        "Allowed next stages: "
        f"{sorted(_ALLOWED_TRANSITIONS[current_stage], key=lambda stage: _STAGE_ORDER[stage])}"
    )


def summarize_lifecycle_transition(current_stage: Stage, next_stage: Stage) -> str:
    resolved_stage = reduce_thesis_stage(current_stage=current_stage, next_stage=next_stage)
    return (
        "Luận điểm chuyển trạng thái từ "
        f"'{stage_label_vi(current_stage)}' sang '{stage_label_vi(resolved_stage)}'."
    )
