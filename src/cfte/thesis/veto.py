from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol
from cfte.models.events import TapeSnapshot, ThesisSignal

@dataclass(slots=True)
class VetoResult:
    is_vetoed: bool
    reason: str | None = None

class VetoRule(Protocol):
    def check(self, snapshot: TapeSnapshot) -> VetoResult:
        ...

class MarketQualityVeto:
    def __init__(self, max_spread_bps: float = 15.0, max_gap_seconds: float = 2.0):
        self.max_spread_bps = max_spread_bps
        self.max_gap_seconds = max_gap_seconds

    def check(self, snapshot: TapeSnapshot) -> VetoResult:
        if snapshot.spread_bps > self.max_spread_bps:
            return VetoResult(is_vetoed=True, reason=f"Spread quá giãn ({snapshot.spread_bps:.2f} bps)")
        
        # Check gap in metadata if available
        gap = snapshot.metadata.get("gap_seconds", 0.0)
        if gap > self.max_gap_seconds:
            return VetoResult(is_vetoed=True, reason=f"Dữ liệu bị trễ ({gap:.1f}s)")
            
        return VetoResult(is_vetoed=False)

class SignalSanityVeto:
    def __init__(self, min_burst: float = 0.5):
        self.min_burst = min_burst

    def check(self, snapshot: TapeSnapshot) -> VetoResult:
        if snapshot.trade_burst < self.min_burst:
            return VetoResult(is_vetoed=True, reason=f"Xung lực quá yếu ({snapshot.trade_burst:.2f}/s)")
        return VetoResult(is_vetoed=False)

class VetoEngine:
    def __init__(self):
        self.rules: list[VetoRule] = [
            MarketQualityVeto(),
            SignalSanityVeto(),
        ]

    def apply(self, signals: list[ThesisSignal], snapshot: TapeSnapshot) -> list[ThesisSignal]:
        # Perform check once per snapshot
        vetoes = [rule.check(snapshot) for rule in self.rules]
        active_vetoes = [v for v in vetoes if v.is_vetoed]
        
        if not active_vetoes:
            return signals

        # If vetoed, we demote or invalidate signals
        # For simplicity in this Phase, we just mark them as DETECTED or INVALIDATED if they were already low
        veto_reason = " | ".join(v.reason for v in active_vetoes)
        
        for s in signals:
            if s.stage in {"CONFIRMED", "ACTIONABLE"}:
                s.stage = "WATCHLIST"
                s.conflicts.append(f"VETO: {veto_reason}")
                s.score = min(s.score, 60.0) # Cap score
                
        return signals
