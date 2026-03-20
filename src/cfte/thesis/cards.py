from __future__ import annotations

from dataclasses import asdict
import re

from cfte.models.events import ThesisSignal
from cfte.thesis.lifecycle import stage_label_vi

_SETUP_LABELS_VI: dict[str, str] = {
    "stealth_accumulation": "Tích lũy âm thầm",
    "breakout_ignition": "Kích hoạt bứt phá",
    "distribution": "Phân phối",
    "failed_breakout": "Bứt phá thất bại",
}

_DIRECTION_LABELS_VI: dict[str, str] = {
    "LONG_BIAS": "LONG 🔼",
    "SHORT_BIAS": "SHORT 🔽",
}

_STAGE_EMOJI: dict[str, str] = {
    "DETECTED": "🔍",
    "WATCHLIST": "👀",
    "CONFIRMED": "✅",
    "ACTIONABLE": "🔥",
    "INVALIDATED": "❌",
    "RESOLVED": "🏁",
}

_RATIO_LABELS = {
    "Bid L1 áp đảo",
    "Lực đỡ bid rõ ràng",
    "Bên mua hụt lực tại L1",
    "Ask L1 ép xuống",
}

_SPREAD_LABELS = {
    "Spread còn mỏng",
    "Spread đủ thanh khoản",
    "Spread nén thuận lợi cho bứt phá",
    "Spread còn giao dịch được",
}

_FLOW_LABELS = {
    "Delta mua dương",
    "Dòng tiền chủ động theo hướng mua",
    "Delta bán âm mạnh",
    "Lực bán phản công sau breakout",
}

_ABSORPTION_LABELS = {
    "Hấp thụ chủ động cao",
    "Thanh khoản hấp thụ tốt trước điểm nổ",
    "Dấu hiệu hấp thụ phía bán",
    "Thanh khoản tạo bẫy breakout",
}

_BURST_LABELS = {
    "Nhịp trade tăng",
    "Nhịp thoát hàng tăng",
    "Có nỗ lực breakout trước đó",
    "Xung lực giao dịch tăng vọt",
}


def _priority_tag(signal: ThesisSignal) -> str:
    if signal.stage == "ACTIONABLE" and signal.score >= 80 and signal.confidence >= 0.80:
        return "ƯU TIÊN CAO"
    if signal.stage == "CONFIRMED":
        return "ĐÁNG CHÚ Ý"
    if signal.stage == "WATCHLIST":
        return "THEO DÕI"
    if signal.stage == "DETECTED":
        return "THĂM DÒ"
    if signal.stage == "INVALIDATED":
        return "LOẠI BỎ"
    if signal.stage == "RESOLVED":
        return "ĐÃ HOÀN TẤT"
    return signal.stage


def _action_hint(signal: ThesisSignal) -> str:
    if signal.stage == "ACTIONABLE":
        return f"Có thể hành động nếu giữ đúng điều kiện vào lệnh: {signal.entry_style}"
    if signal.stage == "CONFIRMED":
        return f"Đã có xác nhận, ưu tiên chờ điểm vào sạch theo plan: {signal.entry_style}"
    if signal.stage == "WATCHLIST":
        return "Chưa vào vội, tiếp tục quan sát để chờ xác nhận tiếp theo."
    if signal.stage == "DETECTED":
        return "Mới phát hiện, chỉ quan sát. Chưa đủ tốt để vào lệnh."
    if signal.stage == "INVALIDATED":
        return "Kịch bản đã hỏng. Không tiếp tục theo tín hiệu này cho tới khi có thesis mới."
    if signal.stage == "RESOLVED":
        return "Kịch bản đã hoàn tất. Chuyển sang đánh giá outcome thay vì vào mới."
    return signal.entry_style or "Theo dõi thêm."


def _extract_number(text: str) -> float | None:
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if match is None:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _compact_number(value: float, *, signed: bool = False) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000:
        body = f"{abs_value / 1_000_000:.2f}M"
    elif abs_value >= 1_000:
        body = f"{abs_value / 1_000:.2f}K"
    else:
        body = f"{abs_value:.2f}"

    if value < 0:
        return f"-{body}"
    if value > 0 and signed:
        return f"+{body}"
    return body


def _format_price_levels(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        try:
            value = float(token)
        except ValueError:
            return token
        if abs(value) < 10:
            return token
        return f"{value:,.2f}"

    return re.sub(r"(?<![A-Za-z])(-?\d+(?:\.\d+)?)", repl, text)


def _format_reason_item(item: str) -> str:
    if ": " not in item:
        return _format_price_levels(item)

    label, raw_value = item.rsplit(": ", 1)
    value = _extract_number(raw_value)
    if value is None:
        return _format_price_levels(item)

    if label in _RATIO_LABELS:
        return f"{label}: {value * 100:.0f}%"
    if label in _SPREAD_LABELS:
        if abs(value) < 0.01:
            return f"{label}: <0.01 bps"
        return f"{label}: {value:.2f} bps"
    if label in _FLOW_LABELS:
        return f"{label}: {_compact_number(value, signed=True)}"
    if label in _ABSORPTION_LABELS:
        return f"{label}: {_compact_number(value)}"
    if label in _BURST_LABELS:
        return f"{label}: {value:.2f}/s"
    return f"{label}: {raw_value}"


def _join_items(items: list[str], *, fallback: str = "N/A", limit: int = 3) -> str:
    if not items:
        return fallback
    formatted = [_format_reason_item(item) for item in items[:limit]]
    if len(items) > limit:
        formatted.append(f"+{len(items) - limit} yếu tố nữa")
    return " | ".join(formatted)


def render_trader_card(signal: ThesisSignal) -> str:
    why_now = _join_items(signal.why_now)
    conflicts = _join_items(signal.conflicts)
    targets = " | ".join(signal.targets) if signal.targets else "N/A"

    setup_label = _SETUP_LABELS_VI.get(signal.setup, signal.setup)
    direction_label = _DIRECTION_LABELS_VI.get(signal.direction, signal.direction)
    stage_emoji = _STAGE_EMOJI.get(signal.stage, "")
    stage_label = stage_label_vi(signal.stage)
    priority = _priority_tag(signal)
    action_hint = _action_hint(signal)
    invalidation = _format_price_levels(signal.invalidation)

    lines: list[str] = []
    if signal.matrix_cell:
        matrix_label = signal.matrix_alias_vi or signal.matrix_cell
        lines.append(
            f"[{signal.thesis_id[:8]}] {signal.instrument_key} | {matrix_label} | Grade {signal.tradability_grade or '?'}"
        )
        flow_bits: list[str] = []
        if signal.flow_state:
            flow_bits.append(signal.flow_state)
        if signal.decision_posture:
            flow_bits.append(f"Posture {signal.decision_posture}")
        if flow_bits:
            lines.append(f"Flow: {' | '.join(flow_bits)}")
        if signal.decision_summary_vi:
            lines.append(f"Decision: {_format_price_levels(signal.decision_summary_vi)}")
        lines.append(f"Setup tương thích: {setup_label} | {direction_label}")
    else:
        lines.append(f"[{signal.thesis_id[:8]}] {setup_label} | {direction_label}")

    lines.extend(
        [
            f"Trạng thái: {stage_emoji} {stage_label} | {priority}",
            f"Hành động: {action_hint}",
            f"Chỉ số: Score {signal.score:>.1f} | Conf {signal.confidence:>.2f} | Cov {signal.coverage:>.2f}",
            f"Cơ sở: {why_now}",
            f"Rủi ro: {conflicts}",
            f"Vô hiệu: {invalidation} | Vào: {signal.entry_style}",
            f"Mục tiêu: {targets}",
        ]
    )
    return "\n".join(lines)


def signal_to_dict(signal: ThesisSignal) -> dict[str, object]:
    return asdict(signal)
