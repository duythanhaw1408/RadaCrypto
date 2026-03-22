from dataclasses import dataclass, field
from typing import Dict, Optional
import random

@dataclass
class ProbabilityEdge:
    setup_name: str
    historical_win_rate: float
    expected_rr: float
    sample_size: int
    edge_score: float # 0 to 1
    confidence: str # LOW, MEDIUM, HIGH

class ProbabilityEngine:
    def __init__(self):
        # Default/Heuristic base stats as fallback
        self._stats_db = {
            "POS_INIT__POS_INV": {"win_rate": 0.62, "rr": 1.5, "count": 0},
            "POS_INIT__NEG_INV": {"win_rate": 0.45, "rr": 2.1, "count": 0},
            "NEG_INIT__NEG_INV": {"win_rate": 0.65, "rr": 1.4, "count": 0},
            "NEG_INIT__POS_INV": {"win_rate": 0.48, "rr": 2.0, "count": 0},
        }
        self._is_ready = False

    def refresh_stats(self, scorecard: list[dict]):
        """Updates internal stats from database scorecard results"""
        for entry in scorecard:
            cell = entry.get("matrix_cell")
            if not cell:
                continue
                
            horizons = entry.get("horizons", {})
            # Prefer 5m horizon for win rate, fallback to any available
            stats_5m = horizons.get("5m") or horizons.get("15m") or horizons.get("1m")
            
            if not stats_5m:
                continue
                
            total = stats_5m.get("count", 0)
            wr = stats_5m.get("win_rate", 0.5)
            # Derive RR from avg_mfe / abs(avg_mae) if possible, or use heuristic
            # For now, let's use a very conservative derivation: 
            # if avg_edge > 0, we trust it more.
            
            current = self._stats_db.get(cell, {"win_rate": 0.5, "rr": 1.5, "count": 0})
            
            if total >= 5: # Minimum sample to trust DB stats
                self._stats_db[cell] = {
                    "win_rate": wr,
                    "rr": current["rr"], # Keep heuristic RR for now until formula refined
                    "count": total
                }
        self._is_ready = True

    def evaluate_edge(self, matrix_cell: str, sequence_length: int = 1) -> ProbabilityEdge:
        """Evaluates the mathematical edge of a specific flow state"""
        base_stats = self._stats_db.get(matrix_cell, {"win_rate": 0.50, "rr": 1.0, "count": 0})
        
        # Adjust win rate based on sequence maturity
        # A sequence of 3-4 is optimal, >5 starts to exhaust
        wr_modifier = 0.0
        if sequence_length == 2:
            wr_modifier = 0.02
        elif 3 <= sequence_length <= 4:
            wr_modifier = 0.05
        elif sequence_length >= 5:
            wr_modifier = -0.05 # Exhaustion risk lowers win rate
            
        final_wr = min(0.95, max(0.05, base_stats["win_rate"] + wr_modifier))
        
        # Edge = (WinRate * RR) - (LossRate * 1)
        loss_rate = 1.0 - final_wr
        edge_raw = (final_wr * base_stats["rr"]) - loss_rate
        
        # Normalize edge score between 0 and 1 (max realistic edge ~ 1.0)
        edge_score = max(0.0, min(1.0, (edge_raw + 0.2) / 1.0)) # mapping -0.2 to 0, 0.8 to 1
        
        if base_stats["count"] >= 100:
            confidence = "HIGH"
        elif base_stats["count"] >= 20: 
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
            
        return ProbabilityEdge(
            setup_name=matrix_cell,
            historical_win_rate=round(final_wr, 2),
            expected_rr=base_stats["rr"],
            sample_size=base_stats["count"],
            edge_score=round(edge_score, 2),
            confidence=confidence
        )
