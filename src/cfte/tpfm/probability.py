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
        # Heuristic fallbacks
        self._cell_stats = {
            "POS_INIT__POS_INV": {"win_rate": 0.62, "rr": 1.5, "count": 0},
            "POS_INIT__NEG_INV": {"win_rate": 0.45, "rr": 2.1, "count": 0},
            "NEG_INIT__NEG_INV": {"win_rate": 0.65, "rr": 1.4, "count": 0},
            "NEG_INIT__POS_INV": {"win_rate": 0.48, "rr": 2.0, "count": 0},
        }
        # Stats-native pattern/sequence cache
        self._pattern_stats = {} # (pattern_code, sequence_signature) -> stats
        self._is_ready = False

    def refresh_stats(self, cell_scorecard: list[dict], pattern_scorecard: list[dict] = None):
        """Updates internal stats from database scorecard results"""
        # 1. Update Cell Heuristics
        for entry in cell_scorecard:
            cell = entry.get("matrix_cell")
            if not cell: continue
            horizons = entry.get("horizons", {})
            stats_5m = horizons.get("5m") or horizons.get("15m") or horizons.get("1m")
            if not stats_5m: continue
            
            if stats_5m.get("count", 0) >= 5:
                self._cell_stats[cell] = {
                    "win_rate": stats_5m.get("win_rate", 0.5),
                    "rr": self._cell_stats.get(cell, {}).get("rr", 1.5),
                    "count": stats_5m.get("count", 0)
                }
        
        # 2. Update Pattern-Native Stats (Phase 14 Uplift)
        if pattern_scorecard:
            for entry in pattern_scorecard:
                key = (entry["pattern_code"], entry["sequence_signature"])
                self._pattern_stats[key] = {
                    "win_rate": entry["win_rate_5m"],
                    "rr": entry["avg_rr"] or 1.5,
                    "count": entry["count"]
                }
        
        self._is_ready = True

    def evaluate_edge(self, matrix_cell: str, sequence_length: int = 1, 
                      pattern_code: str = None, sequence_signature: str = None) -> ProbabilityEdge:
        """Evaluates the mathematical edge using the most specific data available."""
        # Start with matrix-level baseline
        baseline = self._cell_stats.get(matrix_cell, {"win_rate": 0.50, "rr": 1.5, "count": 0})
        
        # Priority 1: Specific Pattern-Native match (Finding 3 Uplift)
        stats = baseline
        source = "CELL_HEURISTIC"
        
        if pattern_code and sequence_signature:
            p_stats = self._pattern_stats.get((pattern_code, sequence_signature))
            if p_stats and p_stats["count"] >= 5: # Minimum threshold to trust pattern-native over cell-native
                stats = p_stats
                source = "PATTERN_NATIVE"
        
        # Win rate adjustment for sequence maturity if using cell baseline
        final_wr = stats["win_rate"]
        if source == "CELL_HEURISTIC":
            wr_mod = 0.0
            if sequence_length == 2: wr_mod = 0.02
            elif 3 <= sequence_length <= 4: wr_mod = 0.05
            elif sequence_length >= 5: wr_mod = -0.05
            final_wr = min(0.95, max(0.05, final_wr + wr_mod))
            
        loss_rate = 1.0 - final_wr
        edge_raw = (final_wr * stats["rr"]) - loss_rate
        
        # Normalize edge score
        edge_score = max(0.0, min(1.0, (edge_raw + 0.2) / 1.0))
        
        # New thresholds: < 5 (LOW), 5-19 (MEDIUM), >= 20 (HIGH)
        if stats["count"] >= 20: 
            confidence = "HIGH"
        elif stats["count"] >= 5: 
            confidence = "MEDIUM"
        else: 
            confidence = "LOW"
        
        return ProbabilityEdge(
            setup_name=pattern_code if source == "PATTERN_NATIVE" else matrix_cell,
            historical_win_rate=round(final_wr, 2),
            expected_rr=round(stats["rr"], 2),
            sample_size=stats["count"],
            edge_score=round(edge_score, 2),
            confidence=confidence
        )
