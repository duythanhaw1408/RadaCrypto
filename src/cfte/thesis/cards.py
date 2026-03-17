from __future__ import annotations

from cfte.models.events import ThesisSignal

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
        "timeframe": signal.timeframe,
        "regime_bucket": signal.regime_bucket,
    }

def render_trader_card(signal: ThesisSignal) -> str:
    why = "\n".join(f"- {x}" for x in signal.why_now) if signal.why_now else "- Không có"
    conflicts = "\n".join(f"- {x}" for x in signal.conflicts) if signal.conflicts else "- Không có"
    targets = ", ".join(signal.targets)
    return (
        f"[{signal.stage}] {signal.instrument_key}\n"
        f"Thiết lập: {signal.setup}\n"
        f"Xu hướng: {signal.direction}\n"
        f"Điểm số: {signal.score:.2f}\n"
        f"Độ tin cậy: {signal.confidence:.2f}\n"
        f"Độ phủ dữ liệu: {signal.coverage:.2f}\n"
        f"Lý do lúc này:\n{why}\n"
        f"Yếu tố mâu thuẫn:\n{conflicts}\n"
        f"Cách vào lệnh: {signal.entry_style}\n"
        f"Điểm vô hiệu: {signal.invalidation}\n"
        f"Mục tiêu: {targets}\n"
    )
