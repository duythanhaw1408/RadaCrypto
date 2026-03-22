from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cfte.thesis.lifecycle import stage_label_vi


@dataclass(frozen=True, slots=True)
class SummaryDocument:
    label: str
    summary_vi: str
    payload: dict[str, Any]


def _fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def _preferred_horizon_stats(row: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    horizons = row.get("horizons", {})
    for name in ("24h", "4h", "1h"):
        if name in horizons:
            return name, horizons[name]
    return None, None


def _matrix_bucket_label(row: dict[str, Any]) -> str:
    return (
        f"{row.get('matrix_alias_vi', row.get('matrix_cell', 'UNKNOWN'))} | "
        f"{row.get('spot_futures_relation', 'N/A')}/{row.get('venue_confirmation_state', 'N/A')}/"
        f"{row.get('liquidation_bias', 'N/A')} | grade {row.get('tradability_grade', 'N/A')}"
    )


def _flow_state_bucket_label(row: dict[str, Any]) -> str:
    return (
        f"{row.get('flow_state_code', 'NO_FLOW_CONTEXT')} | "
        f"{row.get('forced_flow_state', 'NONE')}/{row.get('inventory_defense_state', 'NONE')}/"
        f"{row.get('decision_posture', 'WAIT')} | grade {row.get('tradability_grade', 'N/A')}"
    )


def _forced_flow_bucket_label(row: dict[str, Any]) -> str:
    return (
        f"{row.get('forced_flow_state', 'NONE')} | "
        f"{row.get('liquidation_bias', 'UNKNOWN')}/{row.get('basis_state', 'BALANCED')} | "
        f"grade {row.get('tradability_grade', 'N/A')}"
    )


def _pick_matrix_bucket(rows: list[dict[str, Any]], *, reverse: bool) -> dict[str, Any] | None:
    ranked: list[tuple[float, int, int, dict[str, Any]]] = []
    for row in rows:
        _, horizon_stats = _preferred_horizon_stats(row)
        if not horizon_stats:
            continue
        ranked.append(
            (
                float(horizon_stats.get("avg_edge", 0.0) or 0.0),
                int(horizon_stats.get("count", 0) or 0),
                int(row.get("total_signals", 0) or 0),
                row,
            )
        )
    if not ranked:
        return None
    ranked.sort(reverse=reverse)
    return ranked[0][-1]


def _pick_flow_bucket(rows: list[dict[str, Any]], *, reverse: bool) -> dict[str, Any] | None:
    ranked: list[tuple[float, int, float, dict[str, Any]]] = []
    for row in rows:
        _, horizon_stats = _preferred_horizon_stats(row)
        if not horizon_stats:
            continue
        ranked.append(
            (
                float(horizon_stats.get("avg_edge", 0.0) or 0.0),
                int(horizon_stats.get("count", 0) or 0),
                float(row.get("avg_context_quality_score", 0.0) or 0.0),
                row,
            )
        )
    if not ranked:
        return None
    ranked.sort(reverse=reverse)
    return ranked[0][-1]


def _pick_forced_bucket(rows: list[dict[str, Any]], *, reverse: bool) -> dict[str, Any] | None:
    ranked: list[tuple[float, int, float, dict[str, Any]]] = []
    for row in rows:
        _, horizon_stats = _preferred_horizon_stats(row)
        if not horizon_stats:
            continue
        ranked.append(
            (
                float(horizon_stats.get("avg_edge", 0.0) or 0.0),
                int(horizon_stats.get("count", 0) or 0),
                float(row.get("avg_forced_flow_intensity", 0.0) or 0.0),
                row,
            )
        )
    if not ranked:
        return None
    ranked.sort(reverse=reverse)
    return ranked[0][-1]


def _pick_transition_bucket(rows: list[dict[str, Any]], *, reverse: bool) -> dict[str, Any] | None:
    ranked: list[tuple[float, int, float, dict[str, Any]]] = []
    for row in rows:
        _, horizon_stats = _preferred_horizon_stats(row)
        if not horizon_stats:
            continue
        ranked.append(
            (
                float(horizon_stats.get("avg_edge", 0.0) or 0.0),
                int(horizon_stats.get("count", 0) or 0),
                float(row.get("avg_transition_quality", 0.0) or 0.0),
                row,
            )
        )
    if not ranked:
        return None
    ranked.sort(reverse=reverse)
    return ranked[0][-1]


def render_daily_summary_vi(
    stats: dict[str, Any],
    review_summary: dict[str, Any] | None = None,
    matrix_scorecard: list[dict[str, Any]] | None = None,
    flow_state_scorecard: list[dict[str, Any]] | None = None,
    forced_flow_scorecard: list[dict[str, Any]] | None = None,
    pattern_scorecard: list[dict[str, Any]] | None = None,
) -> str:
    stage_parts = [
        f"{stage_label_vi(stage)}: {count}"
        for stage, count in stats.get("stage_dist", {}).items()
    ]
    closed_parts = [
        f"{stage_label_vi(stage)}: {count}"
        for stage, count in stats.get("closed_stage_dist", {}).items()
    ]
    top_setup = next(iter(stats.get("setup_dist", {})), None)
    hit_rate = (stats.get("positive_outcomes", 0) / stats.get("outcomes_count", 1) * 100.0) if stats.get("outcomes_count", 0) else 0.0
    review_summary = review_summary or {}
    matrix_scorecard = matrix_scorecard or []
    flow_state_scorecard = flow_state_scorecard or []
    forced_flow_scorecard = forced_flow_scorecard or []
    decision_counts = review_summary.get('decision_counts', {})
    usefulness_counts = review_summary.get('usefulness_counts', {})
    fill_rate = (stats.get("fill_count", 0) / stats.get("outcomes_count", 1) * 100.0) if stats.get("outcomes_count", 0) else 0.0
    top_matrix = _pick_matrix_bucket(matrix_scorecard, reverse=True)
    top_flow = _pick_flow_bucket(flow_state_scorecard, reverse=True)
    top_forced = _pick_forced_bucket(forced_flow_scorecard, reverse=True)
    
    # Phase 14: Pattern-native pick
    pattern_scorecard = pattern_scorecard or []
    top_pattern = sorted(pattern_scorecard, key=lambda x: (x.get("win_rate_5m", 0), x.get("count", 0)), reverse=True)[0] if pattern_scorecard else None
    lines = [
        f"Tổng kết ngày {stats.get('label', 'N/A')}",
        f"- Luận điểm mới: {stats.get('opened_count', 0)} | score TB: {stats.get('avg_score', 0.0):.2f} | confidence TB: {stats.get('avg_confidence', 0.0):.2f}",
        f"- Outcome đã chốt: {stats.get('outcomes_count', 0)} | hit rate: {hit_rate:.1f}% | edge TB: {_fmt_pct(stats.get('avg_edge', 0.0))}",
        f"- Realism: khớp {stats.get('fill_count', 0)} lệnh ({fill_rate:.1f}%) | MAE TB: {stats.get('avg_mae', 0.0):.1f}bps | MFE TB: {stats.get('avg_mfe', 0.0):.1f}bps",
        f"- Setup xuất hiện nhiều nhất: {top_setup or 'chưa có'}",
        f"- Trạng thái mở mới: {', '.join(stage_parts) if stage_parts else 'chưa có'}",
        f"- Trạng thái đóng trong ngày: {', '.join(closed_parts) if closed_parts else 'chưa có'}",
        (
            '- Review cá nhân: '
            f"vào={decision_counts.get('taken', 0)}, bỏ={decision_counts.get('skipped', 0)}, phớt lờ={decision_counts.get('ignored', 0)}"
        ),
        (
            '- Hữu ích / nhiễu: '
            f"hữu ích={usefulness_counts.get('useful', 0)}, trung tính={usefulness_counts.get('neutral', 0)}, nhiễu={usefulness_counts.get('noise', 0)}"
        ),
    ]
    if top_matrix is not None:
        horizon, horizon_stats = _preferred_horizon_stats(top_matrix)
        lines.append(
            f"- Matrix nổi bật: {_matrix_bucket_label(top_matrix)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if top_flow is not None:
        horizon, horizon_stats = _preferred_horizon_stats(top_flow)
        lines.append(
            f"- Flow state nổi bật: {_flow_state_bucket_label(top_flow)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if top_forced is not None:
        horizon, horizon_stats = _preferred_horizon_stats(top_forced)
        lines.append(
            f"- Forced flow đáng chú ý: {_forced_flow_bucket_label(top_forced)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if top_pattern:
        lines.append(
            f"- Pattern nổi bật: {top_pattern['pattern_code']} ({top_pattern['sequence_signature']}) | win rate {top_pattern['win_rate_5m']*100:.1f}% | RR {top_pattern['avg_rr']:.2f}"
        )
    return "\n".join(lines)


def render_weekly_review_vi(
    stats: dict[str, Any],
    scorecard: list[dict[str, Any]],
    review_summary: dict[str, Any] | None = None,
    tuning_suggestions: list[dict[str, Any]] | None = None,
    matrix_scorecard: list[dict[str, Any]] | None = None,
    transition_scorecard: list[dict[str, Any]] | None = None,
    flow_state_scorecard: list[dict[str, Any]] | None = None,
    forced_flow_scorecard: list[dict[str, Any]] | None = None,
    flow_state_tuning_suggestions: list[dict[str, Any]] | None = None,
    forced_flow_tuning_suggestions: list[dict[str, Any]] | None = None,
    transition_tuning_suggestions: list[dict[str, Any]] | None = None,
) -> str:
    ranked = sorted(
        scorecard,
        key=lambda row: (row.get("horizons", {}).get("24h", {}).get("avg_edge", float("-inf")), row["total_signals"]),
        reverse=True,
    )
    best = ranked[0]["setup"] if ranked else "chưa có"
    worst = ranked[-1]["setup"] if ranked else "chưa có"
    review_summary = review_summary or {}
    tuning_suggestions = tuning_suggestions or []
    matrix_scorecard = matrix_scorecard or []
    transition_scorecard = transition_scorecard or []
    flow_state_scorecard = flow_state_scorecard or []
    forced_flow_scorecard = forced_flow_scorecard or []
    flow_state_tuning_suggestions = flow_state_tuning_suggestions or []
    forced_flow_tuning_suggestions = forced_flow_tuning_suggestions or []
    transition_tuning_suggestions = transition_tuning_suggestions or []
    setup_rows = review_summary.get('setup_rows', [])
    noisy_from_review = next((row['setup'] for row in sorted(setup_rows, key=lambda item: (item['noise_rate'], item['total']), reverse=True) if row['total'] > 0), 'chưa có')
    top_tuning = None
    if flow_state_tuning_suggestions:
        row = flow_state_tuning_suggestions[0]
        top_tuning = f"{row['flow_state_code']} -> {row['suggested_threshold']:.1f}"
    elif transition_tuning_suggestions:
        row = transition_tuning_suggestions[0]
        top_tuning = f"{row.get('transition_alias_vi', row['transition_code'])} -> {row['suggested_threshold']:.1f}"
    elif forced_flow_tuning_suggestions:
        row = forced_flow_tuning_suggestions[0]
        top_tuning = f"{row['forced_flow_state']} -> {row['suggested_threshold']:.1f}"
    elif tuning_suggestions:
        row = tuning_suggestions[0]
        top_tuning = f"{row['setup']} -> {row['suggested_threshold']:.1f}"
    best_matrix = _pick_matrix_bucket(matrix_scorecard, reverse=True)
    worst_matrix = _pick_matrix_bucket(matrix_scorecard, reverse=False)
    best_transition = _pick_transition_bucket(transition_scorecard, reverse=True)
    worst_transition = _pick_transition_bucket(transition_scorecard, reverse=False)
    best_flow = _pick_flow_bucket(flow_state_scorecard, reverse=True)
    worst_flow = _pick_flow_bucket(flow_state_scorecard, reverse=False)
    best_forced = _pick_forced_bucket(forced_flow_scorecard, reverse=True)
    worst_forced = _pick_forced_bucket(forced_flow_scorecard, reverse=False)
    lines = [
        f"Review tuần {stats['label']}",
        f"- Tổng luận điểm mở mới: {stats['opened_count']} | outcome hoàn tất: {stats['outcomes_count']}",
        f"- Score TB: {stats['avg_score']:.2f} | Confidence TB: {stats['avg_confidence']:.2f} | Edge TB: {_fmt_pct(stats['avg_edge'])}",
        f"- Setup hiệu quả nhất tạm thời: {best}",
        f"- Setup nhiễu nhất tạm thời: {worst}",
        f"- Setup bị chấm nhiễu nhiều nhất trong journal: {noisy_from_review}",
        (
            '- Review cá nhân tuần: '
            f"vào={review_summary.get('decision_counts', {}).get('taken', 0)}, "
            f"bỏ={review_summary.get('decision_counts', {}).get('skipped', 0)}, "
            f"phớt lờ={review_summary.get('decision_counts', {}).get('ignored', 0)}"
        ),
        (
            '- Tuning ưu tiên: '
            f"{top_tuning}"
            if top_tuning else '- Tuning ưu tiên: chưa có dữ liệu'
        ),
        "- Gợi ý review: siết threshold với setup có edge âm, kiểm tra confidence nếu hit rate không tăng theo score.",
    ]
    if best_matrix is not None:
        horizon, horizon_stats = _preferred_horizon_stats(best_matrix)
        lines.append(
            f"- Matrix tốt nhất: {_matrix_bucket_label(best_matrix)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if worst_matrix is not None:
        horizon, horizon_stats = _preferred_horizon_stats(worst_matrix)
        lines.append(
            f"- Matrix yếu nhất: {_matrix_bucket_label(worst_matrix)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if best_flow is not None:
        horizon, horizon_stats = _preferred_horizon_stats(best_flow)
        lines.append(
            f"- Flow state tốt nhất: {_flow_state_bucket_label(best_flow)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if worst_flow is not None:
        horizon, horizon_stats = _preferred_horizon_stats(worst_flow)
        lines.append(
            f"- Flow state yếu nhất: {_flow_state_bucket_label(worst_flow)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if best_transition is not None:
        horizon, horizon_stats = _preferred_horizon_stats(best_transition)
        lines.append(
            f"- Transition tốt nhất: {best_transition.get('transition_alias_vi', best_transition.get('transition_code', 'N/A'))} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if worst_transition is not None:
        horizon, horizon_stats = _preferred_horizon_stats(worst_transition)
        lines.append(
            f"- Transition yếu nhất: {worst_transition.get('transition_alias_vi', worst_transition.get('transition_code', 'N/A'))} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if best_forced is not None:
        horizon, horizon_stats = _preferred_horizon_stats(best_forced)
        lines.append(
            f"- Forced flow tốt nhất: {_forced_flow_bucket_label(best_forced)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    if worst_forced is not None:
        horizon, horizon_stats = _preferred_horizon_stats(worst_forced)
        lines.append(
            f"- Forced flow rủi ro nhất: {_forced_flow_bucket_label(worst_forced)} | {horizon} edge {_fmt_pct(float(horizon_stats.get('avg_edge', 0.0) or 0.0))}"
        )
    return "\n".join(lines)


def render_setup_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập setup scorecard."
    lines = ["Bảng điểm setup", "setup | tổng | resolved/invalidated | 1h (MAE/MFE) | 4h | 24h"]
    for row in rows:
        horizons = row.get("horizons", {})
        def _h(name: str) -> str:
            data = horizons.get(name)
            if not data:
                return "N/A"
            edge = f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"
            if name == "1h":
                return f"{edge} ({data['avg_mae']:.0f}/{data['avg_mfe']:.0f})"
            return edge
        lines.append(
            f"{row['setup']} | {row['total_signals']} | {row['resolved_count']}/{row['invalidated_count']} | {_h('1h')} | {_h('4h')} | {_h('24h')}"
        )
    return "\n".join(lines)


def render_matrix_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập matrix scorecard."
    lines = [
        "Bảng điểm matrix",
        "matrix | context | tổng | 1h | 4h | 24h",
    ]

    def _h(row: dict[str, Any], name: str) -> str:
        data = row.get("horizons", {}).get(name)
        if not data:
            return "N/A"
        return f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"

    for row in rows:
        lines.append(
            (
                f"{row['matrix_alias_vi']} ({row['matrix_cell']}) | "
                f"{row['spot_futures_relation']}/{row['venue_confirmation_state']}/{row['liquidation_bias']}/"
                f"{row['tradability_grade']} | "
                f"{row['total_signals']} | {_h(row, '1h')} | {_h(row, '4h')} | {_h(row, '24h')}"
            )
        )
    return "\n".join(lines)


def render_flow_state_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập flow-state scorecard."
    lines = [
        "Bảng điểm flow state",
        "flow state | context/posture | trap/force | 1h | 4h | 24h",
    ]

    def _h(row: dict[str, Any], name: str) -> str:
        data = row.get("horizons", {}).get(name)
        if not data:
            return "N/A"
        return f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"

    for row in rows:
        context = (
            f"{row.get('forced_flow_state', 'NONE')}/{row.get('inventory_defense_state', 'NONE')}/"
            f"{row.get('decision_posture', 'WAIT')}/{row.get('tradability_grade', 'N/A')}"
        )
        lines.append(
            (
                f"{row.get('flow_state_code', 'NO_FLOW_CONTEXT')} | "
                f"{context} | "
                f"trap={row.get('avg_trap_risk', 0.0):.2f}/force={row.get('avg_forced_flow_intensity', 0.0):.2f} | "
                f"{_h(row, '1h')} | {_h(row, '4h')} | {_h(row, '24h')}"
            )
        )
    return "\n".join(lines)


def render_forced_flow_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập forced-flow scorecard."
    lines = [
        "Bảng điểm forced flow",
        "forced flow | context | intensity/trap | 1h | 4h | 24h",
    ]

    def _h(row: dict[str, Any], name: str) -> str:
        data = row.get("horizons", {}).get(name)
        if not data:
            return "N/A"
        return f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"

    for row in rows:
        context = (
            f"{row.get('liquidation_bias', 'UNKNOWN')}/{row.get('basis_state', 'BALANCED')}/"
            f"{row.get('tradability_grade', 'N/A')}"
        )
        lines.append(
            (
                f"{row.get('forced_flow_state', 'NONE')} | "
                f"{context} | "
                f"force={row.get('avg_forced_flow_intensity', 0.0):.2f}/liq={row.get('avg_liquidation_intensity', 0.0):.2f}/"
                f"trap={row.get('avg_trap_risk', 0.0):.2f} | "
                f"{_h(row, '1h')} | {_h(row, '4h')} | {_h(row, '24h')}"
            )
        )
    return "\n".join(lines)


def render_transition_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập transition scorecard."
    lines = [
        "Bảng điểm transition (chuyển pha)",
        "transition | family | quality/speed | 1h | 4h | 24h",
    ]

    def _h(row: dict[str, Any], name: str) -> str:
        data = row.get("horizons", {}).get(name)
        if not data:
            return "N/A"
        return f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"

    for row in rows:
        lines.append(
            (
                f"{row.get('transition_alias_vi', row['transition_code'])} ({row['transition_code']}) | "
                f"{row.get('transition_family', 'UNKNOWN')} | "
                f"q={row.get('avg_transition_quality', 0.0):.2f}/s={row.get('avg_transition_speed', 0.0):.2f} | "
                f"{_h(row, '1h')} | {_h(row, '4h')} | {_h(row, '24h')}"
            )
        )
    return "\n".join(lines)


def render_pattern_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu pattern để lập pattern scorecard."
    lines = [
        "Bảng điểm Flow Patterns (Mẫu hình dòng tiền)",
        "pattern | sequence | count | win rate (5m) | avg RR",
    ]
    for row in rows:
        lines.append(
            f"{row['pattern_code']} | {row['sequence_signature']} | {row['count']} | "
            f"{row['win_rate_5m']*100:.1f}% | {row['avg_rr']:.2f}"
        )
    return "\n".join(lines)


def persist_summary_document(output_path: str | Path, document: SummaryDocument) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "label": document.label,
        "summary_vi": document.summary_vi,
        **document.payload,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
