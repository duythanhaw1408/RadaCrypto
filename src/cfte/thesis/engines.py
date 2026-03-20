from __future__ import annotations

import hashlib
from dataclasses import replace

from cfte.models.events import Direction, Setup, Stage, TapeSnapshot, ThesisSignal

ACCUMULATION_SETUP: Setup = "stealth_accumulation"
BREAKOUT_IGNITION_SETUP: Setup = "breakout_ignition"
DISTRIBUTION_SETUP: Setup = "distribution"
FAILED_BREAKOUT_SETUP: Setup = "failed_breakout"

LONG_BIAS: Direction = "LONG_BIAS"
SHORT_BIAS: Direction = "SHORT_BIAS"

_SETUP_ADAPTER_LABELS_VI: dict[Setup, str] = {
    ACCUMULATION_SETUP: "Tích lũy âm thầm",
    BREAKOUT_IGNITION_SETUP: "Kích hoạt bứt phá",
    DISTRIBUTION_SETUP: "Phân phối",
    FAILED_BREAKOUT_SETUP: "Bứt phá thất bại",
}

_SETUP_MIN_TRADES: dict[Setup, int] = {
    ACCUMULATION_SETUP: 2,
    BREAKOUT_IGNITION_SETUP: 3,
    DISTRIBUTION_SETUP: 2,
    FAILED_BREAKOUT_SETUP: 2,
}

_SETUP_MAX_SPREAD_BPS: dict[Setup, float] = {
    ACCUMULATION_SETUP: 12.0,
    BREAKOUT_IGNITION_SETUP: 12.0,
    DISTRIBUTION_SETUP: 12.0,
    FAILED_BREAKOUT_SETUP: 14.0,
}

_SETUP_MIN_ACTIONABLE_BURST: dict[Setup, float] = {
    ACCUMULATION_SETUP: 1.8,
    BREAKOUT_IGNITION_SETUP: 2.8,
    DISTRIBUTION_SETUP: 1.8,
    FAILED_BREAKOUT_SETUP: 2.2,
}

_STAGE_ORDER: dict[Stage, int] = {
    "DETECTED": 0,
    "WATCHLIST": 1,
    "CONFIRMED": 2,
    "ACTIONABLE": 3,
    "INVALIDATED": 4,
    "RESOLVED": 5,
}


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _clip_score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _append_unique(items: list[str], message: str) -> None:
    if message not in items:
        items.append(message)


def _merge_stage_ceiling(current: Stage | None, candidate: Stage) -> Stage:
    if current is None:
        return candidate
    return current if _STAGE_ORDER[current] <= _STAGE_ORDER[candidate] else candidate


def _cap_stage(stage: Stage, ceiling: Stage | None) -> Stage:
    if ceiling is None:
        return stage
    return ceiling if _STAGE_ORDER[stage] > _STAGE_ORDER[ceiling] else stage


def _recent_quote_share(snapshot: TapeSnapshot) -> float:
    value = snapshot.metadata.get("recent_quote_share")
    if value is None:
        return 1.0 if snapshot.trade_count else 0.0
    try:
        return _clip01(float(value))
    except (TypeError, ValueError):
        return 1.0 if snapshot.trade_count else 0.0


def _estimate_coverage(signal: ThesisSignal, snapshot: TapeSnapshot) -> float:
    min_trades = float(_SETUP_MIN_TRADES[signal.setup])
    burst_threshold = _SETUP_MIN_ACTIONABLE_BURST[signal.setup]
    spread_limit = _SETUP_MAX_SPREAD_BPS[signal.setup]
    recent_share = _recent_quote_share(snapshot)
    gap_seconds = max(0.0, float(snapshot.metadata.get("gap_seconds", 0.0)))

    trade_ratio = min(1.0, snapshot.trade_count / min_trades)
    freshness_ratio = min(1.0, recent_share / 0.35) if recent_share > 0 else 0.0
    spread_ratio = 1.0 if snapshot.spread_bps <= spread_limit else max(
        0.0,
        1.0 - min(1.0, (snapshot.spread_bps - spread_limit) / max(spread_limit, 1.0)),
    )
    burst_ratio = min(1.0, snapshot.trade_burst / max(burst_threshold, 0.1))
    gap_ratio = 1.0 if gap_seconds <= 1.5 else max(0.0, 1.0 - min(1.0, (gap_seconds - 1.5) / 2.0))

    coverage = 0.35
    coverage += 0.18 * trade_ratio
    coverage += 0.18 * freshness_ratio
    coverage += 0.12 * spread_ratio
    coverage += 0.08 * burst_ratio
    coverage += 0.09 * gap_ratio
    return round(_clip01(coverage), 2)


def _apply_quality_gate(signal: ThesisSignal, snapshot: TapeSnapshot) -> ThesisSignal:
    score = signal.score
    confidence = signal.confidence
    why_now = list(signal.why_now)
    conflicts = list(signal.conflicts)
    coverage = _estimate_coverage(signal, snapshot)
    stage_ceiling: Stage | None = None

    min_trades = _SETUP_MIN_TRADES[signal.setup]
    spread_limit = _SETUP_MAX_SPREAD_BPS[signal.setup]
    burst_threshold = _SETUP_MIN_ACTIONABLE_BURST[signal.setup]
    recent_share = _recent_quote_share(snapshot)
    gap_seconds = max(0.0, float(snapshot.metadata.get("gap_seconds", 0.0)))

    if snapshot.trade_count < min_trades:
        shortage = min_trades - snapshot.trade_count
        score -= 12.0 + (3.0 * shortage)
        confidence -= 0.08
        coverage = max(0.25, round(coverage - 0.18, 2))
        _append_unique(conflicts, f"Cửa sổ mới có {snapshot.trade_count} trade, còn mỏng cho setup này.")
        thin_window_stage: Stage = "DETECTED" if snapshot.trade_count <= 1 else "WATCHLIST"
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, thin_window_stage)

    if snapshot.spread_bps > spread_limit:
        spread_over = snapshot.spread_bps - spread_limit
        score -= min(25.0, 12.0 + (spread_over * 1.5))
        confidence -= 0.08
        coverage = max(0.25, round(coverage - 0.10, 2))
        _append_unique(conflicts, f"Spread {snapshot.spread_bps:.2f} bps vượt vùng tradeable cho setup này.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    if recent_share < 0.20:
        score -= 18.0
        confidence -= 0.08
        coverage = max(0.25, round(coverage - 0.12, 2))
        _append_unique(conflicts, f"Dòng tiền mới chỉ chiếm {recent_share * 100:.0f}% cửa sổ, tín hiệu đã nguội.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")
    elif recent_share < 0.35:
        score -= 8.0
        confidence -= 0.04
        _append_unique(conflicts, f"Dòng tiền mới chỉ chiếm {recent_share * 100:.0f}% cửa sổ, cần thêm nhịp xác nhận.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    if signal.stage in {"CONFIRMED", "ACTIONABLE"} and snapshot.trade_burst < burst_threshold:
        score -= 14.0
        confidence -= 0.06
        coverage = max(0.25, round(coverage - 0.08, 2))
        _append_unique(conflicts, f"Nhịp {snapshot.trade_burst:.2f}/s chưa đủ sạch cho setup này.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    if gap_seconds > 1.5:
        score -= min(12.0, (gap_seconds - 1.5) * 4.0)
        confidence -= 0.04
        coverage = max(0.25, round(coverage - 0.06, 2))
        _append_unique(conflicts, f"Dữ liệu đang trễ {gap_seconds:.1f}s, cần xác nhận lại tape.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    if coverage < 0.60:
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    # HIDDEN FLOW: Venue Confirmation confirmation
    if snapshot.venue_confirmation_state == "CONFIRMED":
        score += 5.0
        confidence += 0.05
        _append_unique(why_now, "Xác nhận liên sàn: Sàn chính (Binance) dẫn dắt và đồng thuận.")
    elif snapshot.venue_confirmation_state == "ALT_LEAD":
        _append_unique(conflicts, f"Cảnh báo: Sàn khác ({snapshot.leader_venue}) đang dẫn nhịp trước Binance.")
    elif snapshot.venue_confirmation_state == "DIVERGENT":
        score -= 15.0
        confidence -= 0.10
        _append_unique(conflicts, "Phân kỳ liên sàn: Chênh lệch VWAP lớn, dòng tiền không đồng nhất.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    # HIDDEN FLOW: Liquidation Pressure
    if snapshot.liquidation_vol > 0:
        if (signal.direction == "LONG_BIAS" and snapshot.liquidation_bias == "SHORTS_FLUSHED"):
            score += 8.0
            confidence += 0.04
            _append_unique(why_now, f"Ép thanh khoản Short: {snapshot.liquidation_vol:,.0f} USDT đã bị quét.")
        elif (signal.direction == "SHORT_BIAS" and snapshot.liquidation_bias == "LONGS_FLUSHED"):
            score += 8.0
            confidence += 0.04
            _append_unique(why_now, f"Dọn thanh khoản Long: {snapshot.liquidation_vol:,.0f} USDT đã bị quét.")
        elif (signal.direction == "LONG_BIAS" and snapshot.liquidation_bias == "LONGS_FLUSHED"):
            score -= 12.0
            _append_unique(conflicts, "Rủi ro: Long bị thanh lý hàng loạt, áp lực bán cưỡng bức.")
        elif (signal.direction == "SHORT_BIAS" and snapshot.liquidation_bias == "SHORTS_FLUSHED"):
            score -= 12.0
            _append_unique(conflicts, "Rủi ro: Short bị thanh lý hàng loạt, áp lực mua cưỡng bức.")

    score = _clip_score(score)
    confidence = round(_clip01(confidence), 2)
    stage = _cap_stage(assign_stage(score=score, confidence=confidence), stage_ceiling)

    return replace(
        signal,
        stage=stage,
        score=score,
        confidence=confidence,
        coverage=coverage,
        why_now=why_now,
        conflicts=conflicts,
    )


def _thesis_id(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def assign_stage(score: float, confidence: float) -> Stage:
    if confidence < 0.62:
        return "WATCHLIST" if score >= 62 else "DETECTED"
    if score >= 78:
        return "ACTIONABLE"
    if score >= 72:
        return "CONFIRMED"
    if score >= 62:
        return "WATCHLIST"
    return "DETECTED"


def _split_instrument(instrument_key: str) -> tuple[str, str]:
    parts = instrument_key.split(":")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return "UNKNOWN", instrument_key


def build_thesis_id(
    instrument_key: str,
    setup: Setup,
    direction: Direction,
    timeframe: str,
    regime_bucket: str,
) -> str:
    venue, symbol = _split_instrument(instrument_key)
    return _thesis_id(symbol, venue, setup, direction, timeframe, regime_bucket)


def _base_confidence(why_now_count: int) -> float:
    return round(0.55 + min(0.4, why_now_count * 0.07), 2)


def score_stealth_accumulation(snapshot: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if snapshot.delta_quote > 0:
        support += 0.30
        why_now.append(f"Delta mua dương: {snapshot.delta_quote:.2f}")
    else:
        conflicts.append("Delta bán đang lấn át")

    if snapshot.imbalance_l1 > 0.55:
        support += 0.18
        why_now.append(f"Bid L1 áp đảo: {snapshot.imbalance_l1:.2f}")
    else:
        conflicts.append("Mất ưu thế bid tại L1")

    if snapshot.trade_burst >= 2.0:
        support += 0.14
        why_now.append(f"Nhịp trade tăng: {snapshot.trade_burst:.2f}/s")

    if snapshot.spread_bps <= 8.0:
        support += 0.12
        why_now.append(f"Spread còn mỏng: {snapshot.spread_bps:.2f} bps")
    else:
        conflicts.append("Spread giãn, khó giữ nhịp")

    if snapshot.absorption_proxy >= 50:
        support += 0.14
        why_now.append(f"Hấp thụ chủ động cao: {snapshot.absorption_proxy:.2f}")

    if snapshot.last_trade_px >= snapshot.microprice:
        support += 0.12
        why_now.append("Giá khớp giữ trên microprice")
    else:
        conflicts.append("Giá khớp trượt dưới microprice")

    score = round(100.0 * _clip01(support), 2)
    return score, _base_confidence(len(why_now)), why_now, conflicts


def score_breakout_ignition(snapshot: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if snapshot.trade_burst >= 2.8:
        support += 0.26
        why_now.append(f"Xung lực giao dịch tăng vọt: {snapshot.trade_burst:.2f}/s")
    else:
        conflicts.append("Xung lực chưa đủ mạnh cho breakout")

    if snapshot.delta_quote > 0:
        support += 0.20
        why_now.append(f"Dòng tiền chủ động theo hướng mua: {snapshot.delta_quote:.2f}")
    else:
        conflicts.append("Dòng tiền mua chưa xác nhận")

    if snapshot.last_trade_px > snapshot.ask_px:
        support += 0.16
        why_now.append("Giá khớp vượt ask hiện tại")
    elif snapshot.last_trade_px >= snapshot.microprice:
        support += 0.08
        why_now.append("Giá khớp nằm trên microprice")
    else:
        conflicts.append("Giá khớp chưa tạo trạng thái bứt phá")

    if snapshot.spread_bps <= 6.0:
        support += 0.12
        why_now.append(f"Spread nén thuận lợi cho bứt phá: {snapshot.spread_bps:.2f} bps")

    if snapshot.imbalance_l1 >= 0.58:
        support += 0.14
        why_now.append(f"Lực đỡ bid rõ ràng: {snapshot.imbalance_l1:.2f}")

    if snapshot.absorption_proxy >= 65:
        support += 0.12
        why_now.append(f"Thanh khoản hấp thụ tốt trước điểm nổ: {snapshot.absorption_proxy:.2f}")

    score = round(100.0 * _clip01(support), 2)
    return score, _base_confidence(len(why_now)), why_now, conflicts


def score_distribution(snapshot: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if snapshot.delta_quote < 0:
        support += 0.30
        why_now.append(f"Delta bán âm mạnh: {snapshot.delta_quote:.2f}")
    else:
        conflicts.append("Delta mua còn chiếm ưu thế")

    if snapshot.imbalance_l1 < 0.45:
        support += 0.20
        why_now.append(f"Ask L1 ép xuống: {snapshot.imbalance_l1:.2f}")
    else:
        conflicts.append("Bid L1 chưa suy yếu")

    if snapshot.trade_burst >= 2.0:
        support += 0.14
        why_now.append(f"Nhịp thoát hàng tăng: {snapshot.trade_burst:.2f}/s")

    if snapshot.spread_bps <= 8.0:
        support += 0.10
        why_now.append(f"Spread đủ thanh khoản: {snapshot.spread_bps:.2f} bps")

    if snapshot.last_trade_px <= snapshot.microprice:
        support += 0.12
        why_now.append("Giá khớp nằm dưới microprice")
    else:
        conflicts.append("Giá khớp chưa thủng microprice")

    if snapshot.absorption_proxy >= 50:
        support += 0.14
        why_now.append(f"Dấu hiệu hấp thụ phía bán: {snapshot.absorption_proxy:.2f}")

    score = round(100.0 * _clip01(support), 2)
    return score, _base_confidence(len(why_now)), why_now, conflicts


def score_failed_breakout(snapshot: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if snapshot.trade_burst >= 2.2:
        support += 0.16
        why_now.append(f"Có nỗ lực breakout trước đó: {snapshot.trade_burst:.2f}/s")

    if snapshot.delta_quote < 0:
        support += 0.24
        why_now.append(f"Lực bán phản công sau breakout: {snapshot.delta_quote:.2f}")
    else:
        conflicts.append("Lực bán phản công chưa rõ")

    if snapshot.last_trade_px < snapshot.microprice:
        support += 0.20
        why_now.append("Giá khớp quay xuống dưới microprice")
    else:
        conflicts.append("Giá chưa thất bại rõ khỏi vùng breakout")

    if snapshot.imbalance_l1 <= 0.44:
        support += 0.16
        why_now.append(f"Bên mua hụt lực tại L1: {snapshot.imbalance_l1:.2f}")

    if snapshot.spread_bps <= 10.0:
        support += 0.10
        why_now.append(f"Spread còn giao dịch được: {snapshot.spread_bps:.2f} bps")

    if snapshot.absorption_proxy >= 45:
        support += 0.12
        why_now.append(f"Thanh khoản tạo bẫy breakout: {snapshot.absorption_proxy:.2f}")

    score = round(100.0 * _clip01(support), 2)
    return score, _base_confidence(len(why_now)), why_now, conflicts


def _build_signal(
    snapshot: TapeSnapshot,
    setup: Setup,
    direction: Direction,
    score: float,
    confidence: float,
    why_now: list[str],
    conflicts: list[str],
    invalidation: str,
    entry_style: str,
    targets: list[str],
) -> ThesisSignal:
    timeframe = "1h"
    regime_bucket = "NEUTRAL"
    return ThesisSignal(
        thesis_id=build_thesis_id(
            instrument_key=snapshot.instrument_key,
            setup=setup,
            direction=direction,
            timeframe=timeframe,
            regime_bucket=regime_bucket,
        ),
        instrument_key=snapshot.instrument_key,
        setup=setup,
        direction=direction,
        stage=assign_stage(score=score, confidence=confidence),
        score=score,
        confidence=confidence,
        coverage=0.80,
        why_now=why_now,
        conflicts=conflicts,
        invalidation=invalidation,
        entry_style=entry_style,
        targets=targets,
        timeframe=timeframe,
        regime_bucket=regime_bucket,
    )


def _infer_flow_bias(matrix_cell: str, flow_state: str) -> str:
    if "LONG" in flow_state or "BUY" in flow_state or "POS_INIT" in matrix_cell:
        return "LONG"
    if "SHORT" in flow_state or "SELL" in flow_state or "NEG_INIT" in matrix_cell:
        return "SHORT"
    return "NEUTRAL"


def _preferred_setup_for_flow(
    *,
    direction: Direction,
    flow_state: str,
    matrix_cell: str,
    decision_posture: str,
    trap_risk: float,
) -> Setup:
    aggressive = decision_posture == "AGGRESSIVE" or any(
        token in flow_state for token in ("CONTINUATION", "FOLLOW_THROUGH", "FORCED_SHORT_SQUEEZE", "FORCED_LONG_FLUSH")
    )
    trap_like = trap_risk >= 0.45 or "TRAP" in flow_state or matrix_cell in {"POS_INIT__NEG_INV", "NEG_INIT__POS_INV"}

    if direction == LONG_BIAS:
        if aggressive and not trap_like:
            return BREAKOUT_IGNITION_SETUP
        return ACCUMULATION_SETUP

    if trap_like:
        return FAILED_BREAKOUT_SETUP
    return DISTRIBUTION_SETUP


def _fallback_decision_posture(flow_bias: str, tradability_grade: str) -> str:
    if flow_bias == "NEUTRAL":
        return "WAIT"
    if tradability_grade == "A":
        return "AGGRESSIVE"
    if tradability_grade in {"B", "C"}:
        return "CONSERVATIVE"
    return "WAIT"


def _flow_adapter_adjustment(
    *,
    signal: ThesisSignal,
    preferred_setup: Setup,
    flow_bias: str,
    decision_posture: str,
) -> tuple[float, float, list[str], list[str], Stage | None, float]:
    score_delta = 0.0
    confidence_delta = 0.0
    why_now: list[str] = []
    conflicts: list[str] = []
    stage_ceiling: Stage | None = None
    alignment_score = 0.50

    signal_bias = "LONG" if signal.direction == LONG_BIAS else "SHORT"
    aligned_direction = flow_bias in {signal_bias, "NEUTRAL"}

    if flow_bias != "NEUTRAL":
        if aligned_direction:
            score_delta += 5.0
            confidence_delta += 0.05
            alignment_score += 0.20
            why_now.append("Đồng thuận TPFM theo đúng chiều dòng tiền chính")
        else:
            score_delta -= 15.0
            confidence_delta -= 0.10
            alignment_score -= 0.30
            conflicts.append("Ngược pha TPFM: chiều setup không còn là quyết định chính")
            stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

    if aligned_direction:
        if signal.setup == preferred_setup:
            score_delta += 6.0
            confidence_delta += 0.04
            alignment_score += 0.20
            why_now.append(f"Setup adapter chính theo flow: {_SETUP_ADAPTER_LABELS_VI[preferred_setup]}")
        else:
            score_delta -= 4.0
            confidence_delta -= 0.02
            alignment_score -= 0.10
            conflicts.append(f"Setup hiện tại chỉ là adapter phụ; flow đang ưu tiên {_SETUP_ADAPTER_LABELS_VI[preferred_setup]}")
            if decision_posture == "CONSERVATIVE":
                stage_ceiling = _merge_stage_ceiling(stage_ceiling, "CONFIRMED")

    if decision_posture == "WAIT":
        score_delta -= 6.0
        confidence_delta -= 0.04
        alignment_score -= 0.10
        conflicts.append("TPFM đang ở posture WAIT, chưa nên để setup dẫn quyết định.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")
    elif decision_posture == "CONSERVATIVE" and signal.stage == "ACTIONABLE":
        conflicts.append("TPFM đang ở posture CONSERVATIVE, setup chỉ nên vào với xác nhận sạch hơn.")
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, "CONFIRMED")

    return score_delta, confidence_delta, why_now, conflicts, stage_ceiling, _clip01(alignment_score)


def _apply_flow_context_to_signals(
    signals: list[ThesisSignal],
    tpfm_snapshot: Any,
) -> list[ThesisSignal]:
    if not tpfm_snapshot:
        return signals

    adjusted_signals: list[ThesisSignal] = []
    matrix_cell = getattr(tpfm_snapshot, "matrix_cell", "UNKNOWN")
    matrix_alias_vi = getattr(tpfm_snapshot, "matrix_alias_vi", matrix_cell)
    flow_state = getattr(tpfm_snapshot, "flow_state_code", "UNKNOWN")
    trap_risk = float(getattr(tpfm_snapshot, "trap_risk", 0.0))
    tradability_grade = getattr(tpfm_snapshot, "tradability_grade", "D")
    flow_bias = _infer_flow_bias(matrix_cell, flow_state)
    raw_decision_posture = getattr(tpfm_snapshot, "decision_posture", "")
    raw_decision_summary = getattr(tpfm_snapshot, "decision_summary_vi", "") or getattr(tpfm_snapshot, "action_plan_vi", "")
    if raw_decision_posture == "WAIT" and raw_decision_summary in {"", "Đứng ngoài", "Đứng ngoài cho tới khi dòng tiền rõ hơn."}:
        decision_posture = _fallback_decision_posture(flow_bias, tradability_grade)
    else:
        decision_posture = raw_decision_posture or _fallback_decision_posture(flow_bias, tradability_grade)
    decision_summary_vi = raw_decision_summary
    flow_entry = getattr(tpfm_snapshot, "entry_condition_vi", "")
    flow_invalidation = getattr(tpfm_snapshot, "invalid_if", "")

    for signal in signals:
        why_now = list(signal.why_now)
        conflicts = list(signal.conflicts)
        score = signal.score
        confidence = signal.confidence
        preferred_setup = _preferred_setup_for_flow(
            direction=signal.direction,
            flow_state=flow_state,
            matrix_cell=matrix_cell,
            decision_posture=decision_posture,
            trap_risk=trap_risk,
        )
        stage_ceiling: Stage | None = None

        score_delta, confidence_delta, adapter_why, adapter_conflicts, adapter_stage_ceiling, alignment_score = _flow_adapter_adjustment(
            signal=signal,
            preferred_setup=preferred_setup,
            flow_bias=flow_bias,
            decision_posture=decision_posture,
        )
        score += score_delta
        confidence += confidence_delta
        stage_ceiling = _merge_stage_ceiling(stage_ceiling, adapter_stage_ceiling)
        for item in adapter_why:
            _append_unique(why_now, item)
        for item in adapter_conflicts:
            _append_unique(conflicts, item)

        _append_unique(why_now, f"Matrix TPFM: {matrix_alias_vi}")
        if decision_summary_vi:
            _append_unique(why_now, f"Decision TPFM: {decision_summary_vi}")

        # Trap Risk Governance
        if trap_risk >= 0.45:
            score -= (trap_risk * 25.0)
            confidence -= 0.15
            _append_unique(conflicts, f"Rủi ro bẫy TPFM cao ({trap_risk:.2f})")
            if trap_risk >= 0.65:
                score = min(score, 60.0)
                stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

        # Tradability Grade Impact
        if tradability_grade == "A":
            score += 3.0
            confidence += 0.03
        elif tradability_grade == "D":
            score -= 10.0
            confidence -= 0.05
            _append_unique(conflicts, "Chất lượng thị trường (Grade D) không đủ để trade setup này.")
            stage_ceiling = _merge_stage_ceiling(stage_ceiling, "WATCHLIST")

        # Setup-specific adapter hints
        is_long = signal.direction == LONG_BIAS
        is_short = signal.direction == SHORT_BIAS
        if signal.setup == BREAKOUT_IGNITION_SETUP and is_long:
            if "CONTINUATION" in flow_state or "FOLLOW_THROUGH" in flow_state:
                score += 5.0
                _append_unique(why_now, "TPFM xác nhận pha tiếp diễn (Follow-through)")

        if signal.setup == FAILED_BREAKOUT_SETUP and is_short:
            if "__TRAP" in flow_state or trap_risk >= 0.5:
                score += 10.0
                _append_unique(why_now, "TPFM xác nhận tín hiệu bẫy (Trap detected)")

        entry_style = signal.entry_style
        if flow_entry and flow_entry != "N/A":
            entry_style = flow_entry
        elif getattr(tpfm_snapshot, "action_plan_vi", ""):
            entry_style = str(getattr(tpfm_snapshot, "action_plan_vi"))

        invalidation = signal.invalidation
        if flow_invalidation and flow_invalidation != "N/A":
            invalidation = flow_invalidation

        score = _clip_score(score)
        confidence = round(_clip01(confidence), 2)
        stage = _cap_stage(assign_stage(score=score, confidence=confidence), stage_ceiling)

        adjusted_signals.append(
            replace(
                signal,
                stage=stage,
                score=score,
                confidence=confidence,
                why_now=why_now,
                conflicts=conflicts,
                invalidation=invalidation,
                entry_style=entry_style,
                flow_state=flow_state,
                matrix_cell=matrix_cell,
                matrix_alias_vi=matrix_alias_vi,
                tradability_grade=tradability_grade,
                decision_posture=decision_posture,
                decision_summary_vi=decision_summary_vi,
                flow_alignment_score=round(alignment_score, 2),
            )
        )
    return adjusted_signals


def evaluate_setups(
    snapshot: TapeSnapshot, 
    tpfm_snapshot: Any = None
) -> list[ThesisSignal]:
    acc_score, acc_confidence, acc_why_now, acc_conflicts = score_stealth_accumulation(snapshot)
    bo_score, bo_confidence, bo_why_now, bo_conflicts = score_breakout_ignition(snapshot)
    dist_score, dist_confidence, dist_why_now, dist_conflicts = score_distribution(snapshot)
    failed_score, failed_confidence, failed_why_now, failed_conflicts = score_failed_breakout(snapshot)

    signals = [
        _build_signal(
            snapshot=snapshot,
            setup=ACCUMULATION_SETUP,
            direction=LONG_BIAS,
            score=acc_score,
            confidence=acc_confidence,
            why_now=acc_why_now,
            conflicts=acc_conflicts,
            invalidation=f"Mất bid hỗ trợ dưới {snapshot.bid_px:.2f}",
            entry_style="Canh hồi về bid và giữ microprice",
            targets=["TP1: 1 ATR giả lập", "TP2: 2 ATR giả lập"],
        ),
        _build_signal(
            snapshot=snapshot,
            setup=BREAKOUT_IGNITION_SETUP,
            direction=LONG_BIAS,
            score=bo_score,
            confidence=bo_confidence,
            why_now=bo_why_now,
            conflicts=bo_conflicts,
            invalidation=f"Thất bại giữ trên microprice {snapshot.microprice:.2f}",
            entry_style="Theo breakout có xác nhận volume và giữ cấu trúc bid",
            targets=["TP1: Mở rộng 1R", "TP2: Mở rộng 2R"],
        ),
        _build_signal(
            snapshot=snapshot,
            setup=DISTRIBUTION_SETUP,
            direction=SHORT_BIAS,
            score=dist_score,
            confidence=dist_confidence,
            why_now=dist_why_now,
            conflicts=dist_conflicts,
            invalidation=f"Giá reclaim lên trên ask {snapshot.ask_px:.2f}",
            entry_style="Canh failed reclaim và lower-high",
            targets=["TP1: 1 ATR giả lập", "TP2: 2 ATR giả lập"],
        ),
        _build_signal(
            snapshot=snapshot,
            setup=FAILED_BREAKOUT_SETUP,
            direction=SHORT_BIAS,
            score=failed_score,
            confidence=failed_confidence,
            why_now=failed_why_now,
            conflicts=failed_conflicts,
            invalidation=f"Giá quay lại trên ask {snapshot.ask_px:.2f} và giữ được",
            entry_style="Ưu tiên vào khi retest thất bại vùng breakout cũ",
            targets=["TP1: Quay về mid", "TP2: Quét thanh khoản đáy gần nhất"],
        ),
    ]
    quality_gated = [_apply_quality_gate(signal, snapshot) for signal in signals]
    
    # Phase 5: Apply vNext Flow Intelligence
    flow_adjusted = _apply_flow_context_to_signals(quality_gated, tpfm_snapshot)
    
    return sorted(
        flow_adjusted,
        key=lambda item: (item.flow_alignment_score, item.score, item.confidence),
        reverse=True,
    )
