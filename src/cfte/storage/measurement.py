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


def render_daily_summary_vi(stats: dict[str, Any], review_summary: dict[str, Any] | None = None) -> str:
    stage_parts = [
        f"{stage_label_vi(stage)}: {count}"
        for stage, count in stats.get("stage_dist", {}).items()
    ]
    closed_parts = [
        f"{stage_label_vi(stage)}: {count}"
        for stage, count in stats.get("closed_stage_dist", {}).items()
    ]
    top_setup = next(iter(stats.get("setup_dist", {})), None)
    hit_rate = (stats["positive_outcomes"] / stats["outcomes_count"] * 100.0) if stats["outcomes_count"] else 0.0
    review_summary = review_summary or {}
    decision_counts = review_summary.get('decision_counts', {})
    usefulness_counts = review_summary.get('usefulness_counts', {})
    lines = [
        f"Tổng kết ngày {stats['label']}",
        f"- Luận điểm mới: {stats['opened_count']} | score TB: {stats['avg_score']:.2f} | confidence TB: {stats['avg_confidence']:.2f}",
        f"- Outcome đã chốt: {stats['outcomes_count']} | hit rate theo hướng: {hit_rate:.1f}% | edge TB: {_fmt_pct(stats['avg_edge'])}",
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
    return "\n".join(lines)


def render_weekly_review_vi(stats: dict[str, Any], scorecard: list[dict[str, Any]], review_summary: dict[str, Any] | None = None, tuning_suggestions: list[dict[str, Any]] | None = None) -> str:
    ranked = sorted(
        scorecard,
        key=lambda row: (row.get("horizons", {}).get("24h", {}).get("avg_edge", float("-inf")), row["total_signals"]),
        reverse=True,
    )
    best = ranked[0]["setup"] if ranked else "chưa có"
    worst = ranked[-1]["setup"] if ranked else "chưa có"
    review_summary = review_summary or {}
    tuning_suggestions = tuning_suggestions or []
    setup_rows = review_summary.get('setup_rows', [])
    noisy_from_review = next((row['setup'] for row in sorted(setup_rows, key=lambda item: (item['noise_rate'], item['total']), reverse=True) if row['total'] > 0), 'chưa có')
    top_tuning = tuning_suggestions[0] if tuning_suggestions else None
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
            f"{top_tuning['setup']} -> {top_tuning['suggested_threshold']:.1f}"
            if top_tuning else '- Tuning ưu tiên: chưa có dữ liệu'
        ),
        "- Gợi ý review: siết threshold với setup có edge âm, kiểm tra confidence nếu hit rate không tăng theo score.",
    ]
    return "\n".join(lines)


def render_setup_scorecard_vi(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Chưa có đủ dữ liệu outcome để lập setup scorecard."
    lines = ["Bảng điểm setup", "setup | tổng | resolved/invalidated | 1h | 4h | 24h"]
    for row in rows:
        horizons = row.get("horizons", {})
        def _h(name: str) -> str:
            data = horizons.get(name)
            if not data:
                return "N/A"
            return f"{data['avg_edge']:+.2f}%/{data['win_rate']*100:.0f}%"
        lines.append(
            f"{row['setup']} | {row['total_signals']} | {row['resolved_count']}/{row['invalidated_count']} | {_h('1h')} | {_h('4h')} | {_h('24h')}"
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
