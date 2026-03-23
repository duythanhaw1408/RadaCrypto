from __future__ import annotations

from dataclasses import dataclass, replace

from cfte.models.events import Stage, ThesisSignal
from cfte.thesis.lifecycle import ACTIVE_STAGES, reduce_thesis_stage, stage_label_vi, summarize_lifecycle_transition


@dataclass(slots=True)
class ThesisEventRecord:
    thesis_id: str
    event_type: str
    from_stage: Stage
    to_stage: Stage
    event_ts: int
    summary_vi: str
    score: float
    confidence: float
    setup: str = ""
    direction: str = ""
    matrix_cell: str = ""
    matrix_alias_vi: str = ""
    flow_state: str = ""
    decision_posture: str = ""
    flow_alignment_score: float = 0.0


@dataclass(slots=True)
class ThesisLifecycleRecord:
    signal: ThesisSignal
    opened_ts: int
    updated_ts: int
    closed_ts: int | None = None


def _transition_signal(signal: ThesisSignal, next_stage: Stage) -> ThesisSignal:
    return replace(signal, stage=next_stage)


def _build_stage_event(signal: ThesisSignal, current_stage: Stage, next_stage: Stage, event_ts: int) -> ThesisEventRecord:
    return ThesisEventRecord(
        thesis_id=signal.thesis_id,
        event_type="stage_transition",
        from_stage=current_stage,
        to_stage=next_stage,
        event_ts=event_ts,
        summary_vi=summarize_lifecycle_transition(current_stage=current_stage, next_stage=next_stage),
        score=signal.score,
        confidence=signal.confidence,
        setup=signal.setup,
        direction=signal.direction,
        matrix_cell=signal.matrix_cell,
        matrix_alias_vi=signal.matrix_alias_vi,
        flow_state=signal.flow_state,
        decision_posture=signal.decision_posture,
        flow_alignment_score=signal.flow_alignment_score,
    )


def _build_open_event(signal: ThesisSignal, event_ts: int) -> ThesisEventRecord:
    return ThesisEventRecord(
        thesis_id=signal.thesis_id,
        event_type="opened",
        from_stage=signal.stage,
        to_stage=signal.stage,
        event_ts=event_ts,
        summary_vi=f"Khởi tạo luận điểm ở trạng thái '{stage_label_vi(signal.stage)}'.",
        score=signal.score,
        confidence=signal.confidence,
        setup=signal.setup,
        direction=signal.direction,
        matrix_cell=signal.matrix_cell,
        matrix_alias_vi=signal.matrix_alias_vi,
        flow_state=signal.flow_state,
        decision_posture=signal.decision_posture,
        flow_alignment_score=signal.flow_alignment_score,
    )


def apply_signal_update(
    state: ThesisLifecycleRecord | None,
    signal: ThesisSignal,
    event_ts: int,
) -> tuple[ThesisLifecycleRecord, list[ThesisEventRecord]]:
    if state is None:
        next_state = ThesisLifecycleRecord(signal=signal, opened_ts=event_ts, updated_ts=event_ts)
        return next_state, [_build_open_event(signal=signal, event_ts=event_ts)]

    current_stage = state.signal.stage
    desired_stage = signal.stage
    next_state = ThesisLifecycleRecord(
        signal=_transition_signal(signal=signal, next_stage=current_stage),
        opened_ts=state.opened_ts,
        updated_ts=event_ts,
        closed_ts=state.closed_ts,
    )
    events: list[ThesisEventRecord] = []

    if current_stage not in ACTIVE_STAGES:
        return ThesisLifecycleRecord(
            signal=state.signal,
            opened_ts=state.opened_ts,
            updated_ts=event_ts,
            closed_ts=state.closed_ts,
        ), events

    if desired_stage not in ACTIVE_STAGES:
        resolved_stage = reduce_thesis_stage(current_stage=current_stage, next_stage=desired_stage)
        next_state.signal = _transition_signal(signal=signal, next_stage=resolved_stage)
        next_state.closed_ts = event_ts
        events.append(_build_stage_event(signal=signal, current_stage=current_stage, next_stage=resolved_stage, event_ts=event_ts))
        return next_state, events

    current_index = ACTIVE_STAGES.index(current_stage)
    desired_index = ACTIVE_STAGES.index(desired_stage)

    if desired_index < current_index:
        invalidated_stage = reduce_thesis_stage(current_stage=current_stage, next_stage="INVALIDATED")
        next_state.signal = _transition_signal(signal=signal, next_stage=invalidated_stage)
        next_state.closed_ts = event_ts
        events.append(_build_stage_event(signal=signal, current_stage=current_stage, next_stage=invalidated_stage, event_ts=event_ts))
        return next_state, events

    if desired_index == current_index:
        next_state.signal = signal
        return next_state, events

    transition_stage = current_stage
    for next_stage in ACTIVE_STAGES[current_index + 1 : desired_index + 1]:
        transition_stage = reduce_thesis_stage(current_stage=transition_stage, next_stage=next_stage)
        events.append(_build_stage_event(signal=signal, current_stage=next_state.signal.stage, next_stage=transition_stage, event_ts=event_ts))
        next_state.signal = _transition_signal(signal=signal, next_stage=transition_stage)

    return next_state, events


def close_signal_state(
    state: ThesisLifecycleRecord,
    next_stage: Stage,
    event_ts: int,
) -> tuple[ThesisLifecycleRecord, ThesisEventRecord | None]:
    resolved_stage = reduce_thesis_stage(current_stage=state.signal.stage, next_stage=next_stage)
    next_signal = _transition_signal(state.signal, next_stage=resolved_stage)
    next_state = ThesisLifecycleRecord(
        signal=next_signal,
        opened_ts=state.opened_ts,
        updated_ts=event_ts,
        closed_ts=event_ts if resolved_stage not in ACTIVE_STAGES else state.closed_ts,
    )

    if resolved_stage == state.signal.stage:
        return next_state, None

    event = _build_stage_event(signal=state.signal, current_stage=state.signal.stage, next_stage=resolved_stage, event_ts=event_ts)
    return next_state, event
