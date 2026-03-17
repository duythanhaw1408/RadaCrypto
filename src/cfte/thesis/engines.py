from __future__ import annotations

import hashlib

from cfte.models.events import TapeSnapshot, ThesisSignal

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))

def _thesis_id(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]

def _stage_from_score(score: float, confidence: float) -> str:
    if confidence < 0.62:
        return "WATCHLIST" if score >= 62 else "DETECTED"
    if score >= 78:
        return "ACTIONABLE"
    if score >= 72:
        return "CONFIRMED"
    if score >= 62:
        return "WATCHLIST"
    return "DETECTED"

def score_stealth_accumulation(s: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if s.delta_quote > 0:
        support += 0.32
        why_now.append(f"Buy delta positive: {s.delta_quote:.2f}")
    else:
        conflicts.append("Sell delta dominates")

    if s.imbalance_l1 > 0.55:
        support += 0.18
        why_now.append(f"Bid imbalance supportive: {s.imbalance_l1:.2f}")
    else:
        conflicts.append("Top-of-book imbalance not supportive")

    if s.trade_burst >= 2.0:
        support += 0.16
        why_now.append(f"Trade burst elevated: {s.trade_burst:.2f}/s")

    if s.spread_bps <= 8.0:
        support += 0.14
        why_now.append(f"Spread acceptable: {s.spread_bps:.2f} bps")
    else:
        conflicts.append("Spread is wide")

    if s.absorption_proxy >= 50:
        support += 0.10
        why_now.append(f"Absorption proxy elevated: {s.absorption_proxy:.2f}")

    if s.last_trade_px >= s.microprice:
        support += 0.10
        why_now.append("Last trade holding above/near microprice")
    else:
        conflicts.append("Last trade below microprice")

    score = round(100.0 * _clip01(support), 2)
    confidence = round(0.55 + min(0.4, len(why_now) * 0.07), 2)
    return score, confidence, why_now, conflicts

def score_distribution(s: TapeSnapshot) -> tuple[float, float, list[str], list[str]]:
    support = 0.0
    conflicts: list[str] = []
    why_now: list[str] = []

    if s.delta_quote < 0:
        support += 0.34
        why_now.append(f"Sell delta negative: {s.delta_quote:.2f}")
    else:
        conflicts.append("Buy delta still dominates")

    if s.imbalance_l1 < 0.45:
        support += 0.18
        why_now.append(f"Ask-side pressure visible: {s.imbalance_l1:.2f}")
    else:
        conflicts.append("Bid imbalance still present")

    if s.trade_burst >= 2.0:
        support += 0.16
        why_now.append(f"Trade burst elevated: {s.trade_burst:.2f}/s")

    if s.spread_bps <= 8.0:
        support += 0.10
        why_now.append(f"Spread tradable: {s.spread_bps:.2f} bps")

    if s.last_trade_px <= s.microprice:
        support += 0.12
        why_now.append("Last trade below/near microprice")
    else:
        conflicts.append("Last trade holding above microprice")

    if s.absorption_proxy >= 50:
        support += 0.10
        why_now.append(f"Distribution pressure proxy elevated: {s.absorption_proxy:.2f}")

    score = round(100.0 * _clip01(support), 2)
    confidence = round(0.55 + min(0.4, len(why_now) * 0.07), 2)
    return score, confidence, why_now, conflicts

def evaluate_setups(s: TapeSnapshot) -> list[ThesisSignal]:
    signals: list[ThesisSignal] = []

    acc_score, acc_conf, acc_why, acc_conflicts = score_stealth_accumulation(s)
    acc_stage = _stage_from_score(acc_score, acc_conf)
    signals.append(
        ThesisSignal(
            thesis_id=_thesis_id(s.instrument_key, "stealth_accumulation", "LONG", "1h", "NEUTRAL"),
            instrument_key=s.instrument_key,
            setup="stealth_accumulation",
            direction="LONG_BIAS",
            stage=acc_stage,
            score=acc_score,
            confidence=acc_conf,
            coverage=0.80,
            why_now=acc_why,
            conflicts=acc_conflicts,
            invalidation=f"Loss of bid support below {s.bid_px:.2f}",
            entry_style="pullback to bid / microprice hold",
            targets=["TP1 1 ATR proxy", "TP2 2 ATR proxy"],
        )
    )

    dist_score, dist_conf, dist_why, dist_conflicts = score_distribution(s)
    dist_stage = _stage_from_score(dist_score, dist_conf)
    signals.append(
        ThesisSignal(
            thesis_id=_thesis_id(s.instrument_key, "distribution", "SHORT", "1h", "NEUTRAL"),
            instrument_key=s.instrument_key,
            setup="distribution",
            direction="SHORT_BIAS",
            stage=dist_stage,
            score=dist_score,
            confidence=dist_conf,
            coverage=0.80,
            why_now=dist_why,
            conflicts=dist_conflicts,
            invalidation=f"Reclaim above ask {s.ask_px:.2f}",
            entry_style="failed reclaim / lower-high retest",
            targets=["TP1 1 ATR proxy", "TP2 2 ATR proxy"],
        )
    )

    return sorted(signals, key=lambda x: (x.score, x.confidence), reverse=True)
