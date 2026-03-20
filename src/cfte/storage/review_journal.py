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
        avg_mae = float(h24.get('avg_mae', 0.0) or 0.0)
        avg_mfe = float(h24.get('avg_mfe', 0.0) or 0.0)
        
        threshold_delta = 0.0
        rationale: list[str] = []
        
        # Strategic Calibration Logic
        if avg_edge < -5.0 or noise_rate >= 0.5:
            threshold_delta += 5.0
            rationale.append('edge âm hoặc bị đánh giá nhiễu nhiều')
        elif avg_edge > 15.0 and win_rate >= 0.6 and useful_rate >= 0.5:
            threshold_delta -= 3.0
            rationale.append('edge dương tốt và trader đánh giá hữu ích')
        
        # SL/TP calibration based on MAE/MFE
        if abs(avg_mae) > 40: # If MAE is deep, maybe the setup is too loose
            threshold_delta += 2.0
            rationale.append(f'MAE sâu ({avg_mae:.0f}bps), cần siết bộ lọc entry')
        
        if avg_mfe > 60: # If MFE is high, setup has potential
             rationale.append(f'MFE hứa hẹn ({avg_mfe:.0f}bps), cân nhắc nới TP')

        if setup_review.get('total', 0) < 3 or int(h24.get('count', 0) or 0) < 3:
            rationale.append('mẫu ít, giữ nguyên')

        suggestions.append({
            'setup': setup,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'avg_edge_24h': avg_edge,
            'win_rate_24h': win_rate,
            'noise_rate': noise_rate,
            'useful_rate': useful_rate,
            'avg_mae': avg_mae,
            'avg_mfe': avg_mfe,
            'sample_size_24h': int(h24.get('count', 0) or 0),
            'review_samples': int(setup_review.get('total', 0) or 0),
            'rationale_vi': rationale or ['dữ liệu trung tính'],
        })
    return sorted(suggestions, key=lambda item: (item['threshold_delta'], item['avg_edge_24h']), reverse=True)


def _preferred_matrix_horizon(row: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    horizons = row.get('horizons', {})
    for name in ('24h', '4h', '1h'):
        if name in horizons:
            return name, horizons[name]
    return None, None


def build_matrix_tuning_suggestions(
    matrix_scorecard: list[dict[str, Any]],
    *,
    base_threshold: float,
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for row in matrix_scorecard:
        horizon, stats = _preferred_matrix_horizon(row)
        if not horizon or not stats:
            continue

        avg_edge = float(stats.get('avg_edge', 0.0) or 0.0)
        win_rate = float(stats.get('win_rate', 0.0) or 0.0)
        avg_mae = float(stats.get('avg_mae', 0.0) or 0.0)
        avg_mfe = float(stats.get('avg_mfe', 0.0) or 0.0)
        sample_size = int(stats.get('count', 0) or 0)
        threshold_delta = 0.0
        rationale: list[str] = []

        relation = str(row.get('spot_futures_relation', 'NO_TPFM_CONTEXT'))
        venue_state = str(row.get('venue_confirmation_state', 'UNCONFIRMED'))
        liquidation_bias = str(row.get('liquidation_bias', 'UNKNOWN'))
        grade = str(row.get('tradability_grade', 'D'))

        if sample_size < 3:
            rationale.append('mẫu ít, giữ nguyên')
        else:
            if avg_edge < -0.5:
                threshold_delta += 6.0
                rationale.append(f'edge {horizon} âm rõ ({avg_edge:+.2f}%)')
            elif avg_edge < 0.0:
                threshold_delta += 3.0
                rationale.append(f'edge {horizon} âm nhẹ ({avg_edge:+.2f}%)')
            elif avg_edge > 1.5 and win_rate >= 0.6:
                threshold_delta -= 3.0
                rationale.append(f'edge {horizon} tốt ({avg_edge:+.2f}%)')

            if venue_state == 'DIVERGENT':
                threshold_delta += 4.0
                rationale.append('đa sàn phân kỳ')
            elif venue_state == 'ALT_LEAD':
                threshold_delta += 2.0
                rationale.append('Binance không dẫn nhịp')
            elif venue_state == 'CONFIRMED' and avg_edge > 0.0:
                threshold_delta -= 1.0
                rationale.append('đa sàn xác nhận')

            if relation in {'DIVERGENT', 'NO_FUTURES_DELTA'}:
                threshold_delta += 3.0
                rationale.append('spot/futures thiếu đồng pha')
            elif relation == 'CONFLUENT' and avg_edge > 0.0:
                threshold_delta -= 1.0
                rationale.append('spot/futures đồng pha')

            if liquidation_bias == 'MIXED':
                threshold_delta += 1.0
                rationale.append('thanh lý hai chiều, cấu trúc nhiễu')
            elif liquidation_bias in {'SHORTS_FLUSHED', 'LONGS_FLUSHED'} and avg_edge > 0.0:
                rationale.append(f'liquidation có hướng: {liquidation_bias}')

            if grade == 'D':
                threshold_delta += 3.0
                rationale.append('grade D, nên siết mạnh')
            elif grade == 'A' and avg_edge > 0.0 and win_rate >= 0.6:
                threshold_delta -= 1.0
                rationale.append('grade A, có thể nới nhẹ')

            if win_rate < 0.4:
                threshold_delta += 2.0
                rationale.append(f'tỷ lệ thắng thấp ({win_rate*100:.0f}%)')
            if avg_mae > 35.0:
                threshold_delta += 2.0
                rationale.append(f'MAE sâu ({avg_mae:.0f}bps)')
            if avg_mfe > 50.0 and avg_edge > 0.0:
                rationale.append(f'MFE tốt ({avg_mfe:.0f}bps), cân nhắc nới TP')

        action = 'keep'
        if threshold_delta >= 4.0:
            action = 'tighten'
        elif threshold_delta <= -2.0:
            action = 'loosen'

        suggestions.append({
            'matrix_cell': row.get('matrix_cell', 'UNKNOWN'),
            'matrix_alias_vi': row.get('matrix_alias_vi', 'Chưa có matrix'),
            'spot_futures_relation': relation,
            'venue_confirmation_state': venue_state,
            'liquidation_bias': liquidation_bias,
            'tradability_grade': grade,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'action': action,
            'horizon': horizon,
            'avg_edge': avg_edge,
            'win_rate': win_rate,
            'avg_mae': avg_mae,
            'avg_mfe': avg_mfe,
            'sample_size': sample_size,
            'rationale_vi': rationale or ['dữ liệu trung tính'],
        })

    return sorted(
        suggestions,
        key=lambda item: (abs(item['threshold_delta']), item['avg_edge']),
        reverse=True,
    )


def _preferred_flow_horizon(row: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    horizons = row.get('horizons', {})
    for name in ('24h', '4h', '1h'):
        if name in horizons:
            return name, horizons[name]
    return None, None


def build_flow_state_tuning_suggestions(
    flow_state_scorecard: list[dict[str, Any]],
    *,
    base_threshold: float,
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for row in flow_state_scorecard:
        horizon, stats = _preferred_flow_horizon(row)
        if not horizon or not stats:
            continue

        flow_state_code = str(row.get('flow_state_code', 'NO_FLOW_CONTEXT'))
        forced_flow_state = str(row.get('forced_flow_state', 'NONE'))
        inventory_defense_state = str(row.get('inventory_defense_state', 'NONE'))
        decision_posture = str(row.get('decision_posture', 'WAIT'))
        grade = str(row.get('tradability_grade', 'D'))
        avg_edge = float(stats.get('avg_edge', 0.0) or 0.0)
        win_rate = float(stats.get('win_rate', 0.0) or 0.0)
        avg_mae = float(stats.get('avg_mae', 0.0) or 0.0)
        avg_mfe = float(stats.get('avg_mfe', 0.0) or 0.0)
        sample_size = int(stats.get('count', 0) or 0)
        avg_trap_risk = float(row.get('avg_trap_risk', 0.0) or 0.0)
        avg_forced_flow_intensity = float(row.get('avg_forced_flow_intensity', 0.0) or 0.0)
        avg_context_quality_score = float(row.get('avg_context_quality_score', 0.0) or 0.0)
        threshold_delta = 0.0
        rationale: list[str] = []

        if sample_size < 3:
            rationale.append('mẫu ít, giữ nguyên')
        else:
            if avg_edge < -0.5:
                threshold_delta += 5.0
                rationale.append(f'edge {horizon} âm rõ ({avg_edge:+.2f}%)')
            elif avg_edge > 1.2 and win_rate >= 0.6:
                threshold_delta -= 2.0
                rationale.append(f'edge {horizon} tốt ({avg_edge:+.2f}%)')

            if avg_trap_risk >= 0.6:
                threshold_delta += 3.0
                rationale.append('trap risk cao, nên siết')
            elif avg_trap_risk <= 0.25 and avg_edge > 0.0:
                rationale.append('trap risk thấp')

            if forced_flow_state == 'GAP_LED':
                threshold_delta += 2.0
                rationale.append('forced flow kiểu gap-led thường thiếu ổn định')
            elif forced_flow_state in {'SQUEEZE_LED', 'LIQUIDATION_LED'} and avg_edge > 0.0 and win_rate >= 0.55:
                threshold_delta -= 1.0
                rationale.append(f'forced flow {forced_flow_state} đang hỗ trợ đúng hướng')

            if inventory_defense_state == 'NONE' and decision_posture != 'WAIT':
                threshold_delta += 1.0
                rationale.append('thiếu inventory defense để chống nhiễu')
            elif inventory_defense_state in {'BID_DEFENSE', 'ASK_DEFENSE'} and avg_edge > 0.0:
                rationale.append(f'có inventory defense: {inventory_defense_state}')

            if grade == 'D':
                threshold_delta += 3.0
                rationale.append('grade D, cần siết mạnh')
            elif grade == 'A' and avg_edge > 0.0 and win_rate >= 0.6:
                threshold_delta -= 1.0
                rationale.append('grade A, có thể nới nhẹ')

            if avg_context_quality_score <= 0.35:
                threshold_delta += 1.0
                rationale.append(f'context quality thấp ({avg_context_quality_score:.2f})')
            elif avg_context_quality_score >= 0.65 and avg_edge > 0.0:
                rationale.append(f'context quality tốt ({avg_context_quality_score:.2f})')

            if avg_forced_flow_intensity >= 0.8 and forced_flow_state != 'NONE':
                rationale.append(f'forced flow intensity cao ({avg_forced_flow_intensity:.2f})')

            if 'TRAP' in flow_state_code:
                threshold_delta += 2.0
                rationale.append('flow state mang tính trap')
            elif any(token in flow_state_code for token in ('LONG_CONTINUATION', 'SHORT_CONTINUATION')):
                rationale.append('flow state continuation')

            if win_rate < 0.4:
                threshold_delta += 2.0
                rationale.append(f'tỷ lệ thắng thấp ({win_rate*100:.0f}%)')
            if avg_mae > 35.0:
                threshold_delta += 2.0
                rationale.append(f'MAE sâu ({avg_mae:.0f}bps)')
            if avg_mfe > 50.0 and avg_edge > 0.0:
                rationale.append(f'MFE tốt ({avg_mfe:.0f}bps), cân nhắc nới TP')

        action = 'keep'
        if threshold_delta >= 4.0:
            action = 'tighten'
        elif threshold_delta <= -2.0:
            action = 'loosen'

        suggestions.append({
            'flow_state_code': flow_state_code,
            'forced_flow_state': forced_flow_state,
            'inventory_defense_state': inventory_defense_state,
            'decision_posture': decision_posture,
            'tradability_grade': grade,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'action': action,
            'horizon': horizon,
            'avg_edge': avg_edge,
            'win_rate': win_rate,
            'avg_mae': avg_mae,
            'avg_mfe': avg_mfe,
            'avg_trap_risk': avg_trap_risk,
            'avg_forced_flow_intensity': avg_forced_flow_intensity,
            'avg_context_quality_score': avg_context_quality_score,
            'sample_size': sample_size,
            'rationale_vi': rationale or ['dữ liệu trung tính'],
        })

    return sorted(
        suggestions,
        key=lambda item: (abs(item['threshold_delta']), item['avg_edge']),
        reverse=True,
    )


def build_forced_flow_tuning_suggestions(
    forced_flow_scorecard: list[dict[str, Any]],
    *,
    base_threshold: float,
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for row in forced_flow_scorecard:
        horizon, stats = _preferred_flow_horizon(row)
        if not horizon or not stats:
            continue

        forced_flow_state = str(row.get('forced_flow_state', 'NONE'))
        liquidation_bias = str(row.get('liquidation_bias', 'UNKNOWN'))
        basis_state = str(row.get('basis_state', 'BALANCED'))
        grade = str(row.get('tradability_grade', 'D'))
        avg_edge = float(stats.get('avg_edge', 0.0) or 0.0)
        win_rate = float(stats.get('win_rate', 0.0) or 0.0)
        avg_mae = float(stats.get('avg_mae', 0.0) or 0.0)
        avg_mfe = float(stats.get('avg_mfe', 0.0) or 0.0)
        sample_size = int(stats.get('count', 0) or 0)
        avg_forced_flow_intensity = float(row.get('avg_forced_flow_intensity', 0.0) or 0.0)
        avg_liquidation_intensity = float(row.get('avg_liquidation_intensity', 0.0) or 0.0)
        avg_trap_risk = float(row.get('avg_trap_risk', 0.0) or 0.0)
        threshold_delta = 0.0
        rationale: list[str] = []

        if forced_flow_state == 'NONE':
            continue

        if sample_size < 3:
            rationale.append('mẫu ít, giữ nguyên')
        else:
            if avg_edge < -0.5:
                threshold_delta += 4.0
                rationale.append(f'edge {horizon} âm rõ ({avg_edge:+.2f}%)')
            elif avg_edge > 1.0 and win_rate >= 0.6:
                threshold_delta -= 1.0
                rationale.append(f'edge {horizon} tốt ({avg_edge:+.2f}%)')

            if forced_flow_state == 'GAP_LED':
                threshold_delta += 2.0
                rationale.append('gap-led thường dễ đảo pha')
            elif forced_flow_state in {'SQUEEZE_LED', 'LIQUIDATION_LED'} and avg_edge > 0.0:
                rationale.append(f'{forced_flow_state} đang dẫn hướng hợp lệ')

            if liquidation_bias == 'MIXED':
                threshold_delta += 2.0
                rationale.append('liquidation hai chiều, cấu trúc nhiễu')
            elif liquidation_bias == 'UNKNOWN':
                threshold_delta += 1.0
                rationale.append('thiếu hướng liquidation rõ ràng')
            else:
                rationale.append(f'liquidation bias: {liquidation_bias}')

            if basis_state not in {'BALANCED', 'ALIGNED'}:
                threshold_delta += 1.0
                rationale.append(f'basis state nhạy cảm: {basis_state}')

            if avg_forced_flow_intensity >= 0.9:
                rationale.append(f'forced flow intensity cao ({avg_forced_flow_intensity:.2f})')
            if avg_liquidation_intensity >= 0.9:
                rationale.append(f'liquidation intensity cao ({avg_liquidation_intensity:.2f})')
            if avg_trap_risk >= 0.55:
                threshold_delta += 2.0
                rationale.append('trap risk cao trong forced flow')

            if grade == 'D':
                threshold_delta += 2.0
                rationale.append('grade D, cần siết')
            elif grade == 'A' and avg_edge > 0.0 and win_rate >= 0.6:
                threshold_delta -= 1.0
                rationale.append('grade A, có thể nới nhẹ')

            if win_rate < 0.4:
                threshold_delta += 2.0
                rationale.append(f'tỷ lệ thắng thấp ({win_rate*100:.0f}%)')
            if avg_mae > 35.0:
                threshold_delta += 2.0
                rationale.append(f'MAE sâu ({avg_mae:.0f}bps)')
            if avg_mfe > 50.0 and avg_edge > 0.0:
                rationale.append(f'MFE tốt ({avg_mfe:.0f}bps), cân nhắc nới TP')

        action = 'keep'
        if threshold_delta >= 4.0:
            action = 'tighten'
        elif threshold_delta <= -2.0:
            action = 'loosen'

        suggestions.append({
            'forced_flow_state': forced_flow_state,
            'liquidation_bias': liquidation_bias,
            'basis_state': basis_state,
            'tradability_grade': grade,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'action': action,
            'horizon': horizon,
            'avg_edge': avg_edge,
            'win_rate': win_rate,
            'avg_mae': avg_mae,
            'avg_mfe': avg_mfe,
            'avg_forced_flow_intensity': avg_forced_flow_intensity,
            'avg_liquidation_intensity': avg_liquidation_intensity,
            'avg_trap_risk': avg_trap_risk,
            'sample_size': sample_size,
            'rationale_vi': rationale or ['dữ liệu trung tính'],
        })

    return sorted(
        suggestions,
        key=lambda item: (abs(item['threshold_delta']), item['avg_edge']),
        reverse=True,
    )


def _preferred_transition_horizon(row: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    horizons = row.get('horizons', {})
    for name in ('24h', '4h', '1h'):
        if name in horizons:
            return name, horizons[name]
    return None, None


def build_transition_tuning_suggestions(
    transition_scorecard: list[dict[str, Any]],
    *,
    base_threshold: float,
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for row in transition_scorecard:
        horizon, stats = _preferred_transition_horizon(row)
        if not horizon or not stats:
            continue

        transition_code = str(row.get('transition_code', 'NO_TRANSITION_CONTEXT'))
        transition_family = str(row.get('transition_family', 'UNKNOWN'))
        transition_alias_vi = str(row.get('transition_alias_vi', transition_code))
        avg_edge = float(stats.get('avg_edge', 0.0) or 0.0)
        win_rate = float(stats.get('win_rate', 0.0) or 0.0)
        avg_mae = float(stats.get('avg_mae', 0.0) or 0.0)
        avg_mfe = float(stats.get('avg_mfe', 0.0) or 0.0)
        sample_size = int(stats.get('count', 0) or 0)
        avg_transition_quality = float(row.get('avg_transition_quality', 0.0) or 0.0)
        avg_transition_speed = float(row.get('avg_transition_speed', 0.0) or 0.0)
        avg_persistence_score = float(row.get('avg_persistence_score', 0.0) or 0.0)
        avg_trap_risk = float(row.get('avg_trap_risk', 0.0) or 0.0)
        forced_ratio = float(row.get('forced_ratio', 0.0) or 0.0)
        threshold_delta = 0.0
        rationale: list[str] = []

        if sample_size < 3:
            rationale.append('mẫu ít, giữ nguyên')
        else:
            if avg_edge < -0.5:
                threshold_delta += 5.0
                rationale.append(f'edge {horizon} âm rõ ({avg_edge:+.2f}%)')
            elif avg_edge > 1.0 and win_rate >= 0.6:
                threshold_delta -= 2.0
                rationale.append(f'edge {horizon} tốt ({avg_edge:+.2f}%)')

            if transition_family in {'TRAP', 'TRAP_FLIP'} or avg_trap_risk >= 0.55:
                threshold_delta += 3.0
                rationale.append('transition mang tính trap, nên siết')
            elif transition_family in {'CONTINUATION', 'INVENTORY_CONFIRM'} and avg_edge > 0.0:
                threshold_delta -= 1.0
                rationale.append('transition continuation/confirm có lợi thế')
            elif transition_family == 'FORCED':
                rationale.append('transition bị dẫn bởi forced flow, cần theo sát liquidation')

            if avg_transition_quality >= 0.65 and avg_edge > 0.0:
                threshold_delta -= 1.0
                rationale.append(f'chất lượng transition tốt ({avg_transition_quality:.2f})')
            elif avg_transition_quality <= 0.35:
                threshold_delta += 1.0
                rationale.append(f'chất lượng transition thấp ({avg_transition_quality:.2f})')

            if avg_transition_speed >= 0.65 and avg_persistence_score >= 0.5 and avg_edge > 0.0:
                rationale.append('transition nhanh và giữ được quán tính')
            elif avg_transition_speed >= 0.65 and avg_persistence_score < 0.3:
                threshold_delta += 1.0
                rationale.append('transition nhanh nhưng thiếu persistence')

            if forced_ratio >= 0.5:
                rationale.append(f'forced flow chiếm ưu thế ({forced_ratio*100:.0f}%)')

            if win_rate < 0.4:
                threshold_delta += 2.0
                rationale.append(f'tỷ lệ thắng thấp ({win_rate*100:.0f}%)')
            if avg_mae > 35.0:
                threshold_delta += 2.0
                rationale.append(f'MAE sâu ({avg_mae:.0f}bps)')
            if avg_mfe > 50.0 and avg_edge > 0.0:
                rationale.append(f'MFE tốt ({avg_mfe:.0f}bps), cân nhắc nới TP')

        action = 'keep'
        if threshold_delta >= 4.0:
            action = 'tighten'
        elif threshold_delta <= -2.0:
            action = 'loosen'

        suggestions.append({
            'transition_code': transition_code,
            'transition_family': transition_family,
            'transition_alias_vi': transition_alias_vi,
            'current_threshold': base_threshold,
            'suggested_threshold': max(50.0, min(95.0, base_threshold + threshold_delta)),
            'threshold_delta': threshold_delta,
            'action': action,
            'horizon': horizon,
            'avg_edge': avg_edge,
            'win_rate': win_rate,
            'avg_mae': avg_mae,
            'avg_mfe': avg_mfe,
            'avg_transition_quality': avg_transition_quality,
            'avg_transition_speed': avg_transition_speed,
            'avg_persistence_score': avg_persistence_score,
            'avg_trap_risk': avg_trap_risk,
            'forced_ratio': forced_ratio,
            'sample_size': sample_size,
            'rationale_vi': rationale or ['dữ liệu trung tính'],
        })

    return sorted(
        suggestions,
        key=lambda item: (abs(item['threshold_delta']), item['avg_edge']),
        reverse=True,
    )


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


def render_tuning_report_vi(
    suggestions: list[dict[str, Any]],
    matrix_suggestions: list[dict[str, Any]] | None = None,
    transition_suggestions: list[dict[str, Any]] | None = None,
    flow_state_suggestions: list[dict[str, Any]] | None = None,
    forced_flow_suggestions: list[dict[str, Any]] | None = None,
) -> str:
    matrix_suggestions = matrix_suggestions or []
    suggestions = suggestions or []
    transition_suggestions = transition_suggestions or []
    flow_state_suggestions = flow_state_suggestions or []
    forced_flow_suggestions = forced_flow_suggestions or []
    
    if not suggestions and not matrix_suggestions and not transition_suggestions and not flow_state_suggestions and not forced_flow_suggestions:
        return 'Chưa có dữ liệu scorecard để gợi ý tuning threshold.'

    lines = ['Gợi ý tuning threshold cá nhân']

    if flow_state_suggestions:
        lines.append('Theo flow state (Ưu tiên cao nhất)')
        lines.append('flow state | posture/context | hiện tại | gợi ý | edge | win rate | mẫu | lý do')
        for row in flow_state_suggestions:
            context = (
                f"{row['forced_flow_state']}/{row['inventory_defense_state']}/"
                f"{row['decision_posture']}/{row['tradability_grade']}"
            )
            lines.append(
                (
                    f"{row['flow_state_code']} | {context} | "
                    f"{row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                    f"{row['avg_edge']:+.2f}%/{row['horizon']} | {row['win_rate']*100:.0f}% | "
                    f"{row['sample_size']} | {'; '.join(row['rationale_vi'])}"
                )
            )
        lines.append('')

    if transition_suggestions:
        lines.append('Theo transition (Chuyển pha)')
        lines.append('transition | family | hiện tại | gợi ý | edge | win rate | mẫu | lý do')
        for row in transition_suggestions:
            lines.append(
                (
                    f"{row.get('transition_alias_vi', row['transition_code'])} ({row['transition_code']}) | "
                    f"{row.get('transition_family', 'UNKNOWN')} | "
                    f"{row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                    f"{row['avg_edge']:+.2f}%/{row['horizon']} | {row['win_rate']*100:.0f}% | "
                    f"{row['sample_size']} | {'; '.join(row['rationale_vi'])}"
                )
            )
        lines.append('')

    if forced_flow_suggestions:
        lines.append('Theo forced flow')
        lines.append('forced flow | context | hiện tại | gợi ý | edge | win rate | mẫu | lý do')
        for row in forced_flow_suggestions:
            context = f"{row['liquidation_bias']}/{row['basis_state']}/{row['tradability_grade']}"
            lines.append(
                (
                    f"{row['forced_flow_state']} | {context} | "
                    f"{row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                    f"{row['avg_edge']:+.2f}%/{row['horizon']} | {row['win_rate']*100:.0f}% | "
                    f"{row['sample_size']} | {'; '.join(row['rationale_vi'])}"
                )
            )
        lines.append('')

    if matrix_suggestions:
        lines.append('Theo matrix/context')
        lines.append('matrix | context | hiện tại | gợi ý | edge | win rate | mẫu | lý do')
        for row in matrix_suggestions:
            context = (
                f"{row['spot_futures_relation']}/{row['venue_confirmation_state']}/"
                f"{row['liquidation_bias']}/{row['tradability_grade']}"
            )
            lines.append(
                (
                    f"{row['matrix_alias_vi']} ({row['matrix_cell']}) | {context} | "
                    f"{row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                    f"{row['avg_edge']:+.2f}%/{row['horizon']} | {row['win_rate']*100:.0f}% | "
                    f"{row['sample_size']} | {'; '.join(row['rationale_vi'])}"
                )
            )
        lines.append('')

    if suggestions:
        lines.append('Theo setup (Tham khảo)')
        lines.append('setup | hiện tại | gợi ý | edge 24h | win rate | MAE/MFE | lý do')
        for row in suggestions:
            lines.append(
                (
                    f"{row['setup']} | {row['current_threshold']:.1f} | {row['suggested_threshold']:.1f} | "
                    f"{row['avg_edge_24h']:+.2f}% | {row['win_rate_24h']*100:.0f}% | "
                    f"{row['avg_mae']:.0f}/{row['avg_mfe']:.0f} | "
                    f"{'; '.join(row['rationale_vi'])}"
                )
            )
            
    return '\n'.join(lines)
