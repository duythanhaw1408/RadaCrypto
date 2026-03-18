from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_VALID_DECISIONS = {"taken", "skipped", "ignored"}
_VALID_USEFULNESS = {"useful", "neutral", "noise"}


@dataclass(frozen=True, slots=True)
class ReviewDecision:
    thesis_id: str
    decision: str
    usefulness: str
    review_ts: int
    setup: str | None = None
    instrument_key: str | None = None
    note: str | None = None
    tags: tuple[str, ...] = ()
    profile_name: str | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "thesis_id": self.thesis_id,
            "decision": self.decision,
            "usefulness": self.usefulness,
            "review_ts": self.review_ts,
            "setup": self.setup,
            "instrument_key": self.instrument_key,
            "note": self.note,
            "tags": list(self.tags),
            "profile": self.profile_name,
        }


class ReviewJournal:
    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)

    def append(self, decision: ReviewDecision) -> Path:
        if decision.decision not in _VALID_DECISIONS:
            raise ValueError(f"decision không hợp lệ: {decision.decision}")
        if decision.usefulness not in _VALID_USEFULNESS:
            raise ValueError(f"usefulness không hợp lệ: {decision.usefulness}")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(decision.to_record(), ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
        return self.output_path

    def read_records(self) -> list[dict[str, Any]]:
        if not self.output_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with self.output_path.open('r', encoding='utf-8') as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows


def summarize_review_journal(
    rows: list[dict[str, Any]],
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> dict[str, Any]:
    filtered = [
        row for row in rows
        if (start_ts is None or int(row.get('review_ts', 0)) >= start_ts)
        and (end_ts is None or int(row.get('review_ts', 0)) < end_ts)
    ]
    decisions = Counter(str(row.get('decision', 'unknown')) for row in filtered)
    usefulness = Counter(str(row.get('usefulness', 'unknown')) for row in filtered)
    by_setup: dict[str, dict[str, Any]] = defaultdict(lambda: {
        'total': 0,
        'decisions': Counter(),
        'usefulness': Counter(),
    })
    for row in filtered:
        setup = str(row.get('setup') or 'unknown')
        bucket = by_setup[setup]
        bucket['total'] += 1
        bucket['decisions'][str(row.get('decision', 'unknown'))] += 1
        bucket['usefulness'][str(row.get('usefulness', 'unknown'))] += 1
    setup_rows: list[dict[str, Any]] = []
    for setup, bucket in sorted(by_setup.items(), key=lambda item: (-item[1]['total'], item[0])):
        total = int(bucket['total'])
        useful_count = int(bucket['usefulness']['useful'])
        noise_count = int(bucket['usefulness']['noise'])
        setup_rows.append({
            'setup': setup,
            'total': total,
            'taken': int(bucket['decisions']['taken']),
            'skipped': int(bucket['decisions']['skipped']),
            'ignored': int(bucket['decisions']['ignored']),
            'useful_count': useful_count,
            'noise_count': noise_count,
            'useful_rate': (useful_count / total) if total else 0.0,
            'noise_rate': (noise_count / total) if total else 0.0,
        })
    return {
        'total_reviews': len(filtered),
        'decision_counts': dict(decisions),
        'usefulness_counts': dict(usefulness),
        'setup_rows': setup_rows,
    }


def build_tuning_suggestions(
    scorecard: list[dict[str, Any]],
    review_summary: dict[str, Any],
    *,
    base_threshold: float,
) -> list[dict[str, Any]]:
    review_map = {row['setup']: row for row in review_summary.get('setup_rows', [])}
    suggestions: list[dict[str, Any]] = []
    for row in scorecard:
        setup = str(row['setup'])
        setup_review = review_map.get(setup, {})
        h24 = row.get('horizons', {}).get('24h', {})
        avg_edge = float(h24.get('avg_edge', 0.0) or 0.0)
        win_rate = float(h24.get('win_rate', 0.0) or 0.0)
        noise_rate = float(setup_review.get('noise_rate', 0.0) or 0.0)
        useful_rate = float(setup_review.get('useful_rate', 0.0) or 0.0)
        threshold_delta = 0.0
        rationale: list[str] = []
        if avg_edge < 0 or noise_rate >= 0.5:
            threshold_delta += 5.0
            rationale.append('edge âm hoặc bị đánh giá nhiễu nhiều')
        elif avg_edge > 0.75 and win_rate >= 0.6 and useful_rate >= 0.5:
            threshold_delta -= 3.0
            rationale.append('edge dương tốt và trader đánh giá hữu ích')
        elif setup_review.get('total', 0) < 3 or int(h24.get('count', 0) or 0) < 3:
            rationale.append('chưa đủ mẫu, giữ ngưỡng hiện tại')

        suggestions.append({
            'setup': setup,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'avg_edge_24h': avg_edge,
            'win_rate_24h': win_rate,
            'noise_rate': noise_rate,
            'useful_rate': useful_rate,
            'sample_size_24h': int(h24.get('count', 0) or 0),
            'review_samples': int(setup_review.get('total', 0) or 0),
            'rationale_vi': rationale or ['dữ liệu trung tính, chưa cần đổi ngưỡng'],
        })
    return sorted(suggestions, key=lambda item: (item['threshold_delta'], item['avg_edge_24h']), reverse=True)


def render_review_journal_vi(summary: dict[str, Any]) -> str:
    decisions = summary.get('decision_counts', {})
    usefulness = summary.get('usefulness_counts', {})
    lines = [
        'Nhật ký review cá nhân',
        (
            '- Quyết định: '
            f"vào lệnh={decisions.get('taken', 0)}, "
            f"bỏ qua={decisions.get('skipped', 0)}, "
            f"phớt lờ={decisions.get('ignored', 0)}"
        ),
        (
            '- Độ hữu ích: '
            f"hữu ích={usefulness.get('useful', 0)}, "
            f"trung tính={usefulness.get('neutral', 0)}, "
            f"nhiễu={usefulness.get('noise', 0)}"
        ),
    ]
    for row in summary.get('setup_rows', [])[:5]:
        lines.append(
            (
                f"- {row['setup']}: {row['total']} review | vào={row['taken']} | "
                f"bỏ={row['skipped']} | nhiễu={row['noise_count']} | hữu ích={row['useful_count']}"
            )
        )
    return '\n'.join(lines)


def render_tuning_report_vi(suggestions: list[dict[str, Any]]) -> str:
    if not suggestions:
        return 'Chưa có dữ liệu scorecard để gợi ý tuning threshold.'
    lines = ['Gợi ý tuning threshold cá nhân', 'setup | hiện tại | gợi ý | edge 24h | win rate | noise | lý do']
    for row in suggestions:
        lines.append(
            (
                f"{row['setup']} | {row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                f"{row['avg_edge_24h']:+.2f}% | {row['win_rate_24h']*100:.0f}% | {row['noise_rate']*100:.0f}% | "
                f"{'; '.join(row['rationale_vi'])}"
            )
        )
    return '\n'.join(lines)
