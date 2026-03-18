from __future__ import annotations

from cfte.models.events import ThesisSignal
from cfte.thesis.lifecycle import stage_label_vi

_SETUP_LABELS_VI: dict[str, str] = {
    "stealth_accumulation": "Tích lũy âm thầm",
    "breakout_ignition": "Kích hoạt bứt phá",
    "distribution": "Phân phối",
    "failed_breakout": "Bứt phá thất bại",
}

_DIRECTION_LABELS_VI: dict[str, str] = {
    "LONG_BIAS": "Ưu tiên kịch bản tăng",
    "SHORT_BIAS": "Ưu tiên kịch bản giảm",
}


def signal_to_dict(signal: ThesisSignal) -> dict:
    return {
        "thesis_id": signal.thesis_id,
        "instrument_key": signal.instrument_key,
        "setup": signal.setup,
        "direction": signal.direction,
        "stage": signal.stage,
        "score": signal.score,
        "confidence": signal.confidence,
        "coverage": signal.coverage,
        "why_now": signal.why_now,
        "conflicts": signal.conflicts,
        "invalidation": signal.invalidation,
        "entry_style": signal.entry_style,
        "targets": signal.targets,
    }


def render_trader_card(signal: ThesisSignal) -> str:
    why_now = "\n".join(f"- {line}" for line in signal.why_now) if signal.why_now else "- Không có"
    conflicts = "\n".join(f"- {line}" for line in signal.conflicts) if signal.conflicts else "- Không có"
    targets = "\n".join(f"- {line}" for line in signal.targets) if signal.targets else "- Chưa xác định"
    setup_label = _SETUP_LABELS_VI.get(signal.setup, signal.setup)
    direction_label = _DIRECTION_LABELS_VI.get(signal.direction, signal.direction)
    stage_label = stage_label_vi(signal.stage)
    return (
        f"Mã luận điểm: {signal.thesis_id}\n"
        f"Cặp giao dịch: {signal.instrument_key}\n"
        f"Thiết lập: {setup_label}\n"
        f"Xu hướng: {direction_label}\n"
        f"Trạng thái luận điểm: {stage_label}\n"
        f"Điểm số: {signal.score:.2f}\n"
        f"Độ tin cậy: {signal.confidence:.2f}\n"
        f"Độ phủ: {signal.coverage:.2f}\n"
        f"Lý do lúc này:\n{why_now}\n"
        f"Yếu tố mâu thuẫn:\n{conflicts}\n"
        f"Điểm vô hiệu: {signal.invalidation}\n"
        f"Cách vào lệnh: {signal.entry_style}\n"
        f"Mục tiêu:\n{targets}"
    )
