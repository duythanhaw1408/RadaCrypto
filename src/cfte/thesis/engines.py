from __future__ import annotations

import hashlib

from cfte.models.events import Direction, Setup, Stage, TapeSnapshot, ThesisSignal

ACCUMULATION_SETUP: Setup = "stealth_accumulation"
DISTRIBUTION_SETUP: Setup = "distribution"

LONG_BIAS: Direction = "LONG_BIAS"
SHORT_BIAS: Direction = "SHORT_BIAS"


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


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
    confidence = round(0.55 + min(0.4, len(why_now) * 0.07), 2)
    return score, confidence, why_now, conflicts


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
    confidence = round(0.55 + min(0.4, len(why_now) * 0.07), 2)
    return score, confidence, why_now, conflicts


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


def evaluate_setups(snapshot: TapeSnapshot) -> list[ThesisSignal]:
    acc_score, acc_confidence, acc_why_now, acc_conflicts = score_stealth_accumulation(snapshot)
    dist_score, dist_confidence, dist_why_now, dist_conflicts = score_distribution(snapshot)

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
    ]
    return sorted(signals, key=lambda item: (item.score, item.confidence), reverse=True)
