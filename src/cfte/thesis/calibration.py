from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Any

@dataclass(slots=True)
class CalibrationSuggestion:
    setup: str
    regime: str
    current_threshold: float
    suggested_threshold: float
    expectancy: float
    confidence_delta: float
    reason_vi: str

class CalibrationEngine:
    def __init__(self, min_samples: int = 5):
        self.min_samples = min_samples

    def analyze_outcomes(self, outcomes: List[Dict[str, Any]]) -> List[CalibrationSuggestion]:
        """
        outcomes list usually contains rows from thesis_outcome joined with thesis.
        """
        if len(outcomes) < self.min_samples:
            return []

        # Group by setup and regime
        groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for o in outcomes:
            key = (o["setup"], o["regime_bucket"])
            groups.setdefault(key, []).append(o)

        suggestions = []
        for (setup, regime), records in groups.items():
            if len(records) < self.min_samples:
                continue
            
            # Simple Expectancy: (Profit/Loss check)
            # realized_px vs entry_px
            total_profit = 0.0
            wins = 0
            for r in records:
                entry = r.get("entry_px") or r.get("mid_px") # fallback
                exit = r.get("realized_px")
                if not entry or not exit:
                    continue
                
                direction = 1.0 if r["direction"] == "LONG_BIAS" else -1.0
                profit_bps = ((exit - entry) / entry) * 10000.0 * direction
                total_profit += profit_bps
                if profit_bps > 0:
                    wins += 1
            
            count = len(records)
            expectancy = total_profit / count
            win_rate = wins / count
            
            # Logic for threshold suggestion:
            # If expectancy is negative, suggest increasing threshold by 5 points
            # If expectancy is high (> 20bps), suggest decreasing by 2 points to capture more trades
            # If expectancy is mediocre, suggest keeping
            
            curr_threshold = 75.0 # Default fallback
            sugg_threshold = curr_threshold
            reason = "Hiệu suất ổn định."
            
            if expectancy < -5.0:
                sugg_threshold = curr_threshold + 5.0
                reason = f"Kỳ vọng âm ({expectancy:.1f}bps), cần nâng ngưỡng lọc để giảm nhiễu."
            elif expectancy > 15.0 and win_rate > 0.6:
                sugg_threshold = curr_threshold - 3.0
                reason = f"Kỳ vọng tốt ({expectancy:.1f}bps), có thể hạ ngưỡng để tăng cơ hội."
            elif win_rate < 0.4:
                sugg_threshold = curr_threshold + 10.0
                reason = f"Tỷ lệ thắng thấp ({win_rate*100:.0f}%), nâng ngưỡng lọc mạnh."

            if sugg_threshold != curr_threshold:
                suggestions.append(CalibrationSuggestion(
                    setup=setup,
                    regime=regime,
                    current_threshold=curr_threshold,
                    suggested_threshold=sugg_threshold,
                    expectancy=expectancy,
                    confidence_delta=0.0,
                    reason_vi=reason
                ))
                
        return suggestions
