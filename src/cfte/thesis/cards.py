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


def render_trader_card(signal: ThesisSignal) -> str:
    why_now = " | ".join(signal.why_now) if signal.why_now else "N/A"
    conflicts = " | ".join(signal.conflicts) if signal.conflicts else "N/A"
    targets = " | ".join(signal.targets) if signal.targets else "N/A"
    
    setup_label = _SETUP_LABELS_VI.get(signal.setup, signal.setup)
    direction_label = _DIRECTION_LABELS_VI.get(signal.direction, signal.direction)
    stage_emoji = _STAGE_EMOJI.get(signal.stage, "")
    stage_label = stage_label_vi(signal.stage)

    return (
        f"[{signal.thesis_id[:8]}] {signal.instrument_key} | {setup_label}\n"
        f"Bias: {direction_label} | {stage_emoji} {stage_label}\n"
        f"Điểm: {signal.score:>.1f} | Tin cậy: {signal.confidence:>.2f} | Độ phủ: {signal.coverage:>.2f}\n"
        f"Cơ sở: {why_now}\n"
        f"Rủi ro: {conflicts}\n"
        f"Vô hiệu: {signal.invalidation} | Vào lệnh: {signal.entry_style}\n"
        f"Mục tiêu: {targets}"
    )
