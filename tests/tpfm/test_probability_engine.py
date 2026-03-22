import pytest
from cfte.tpfm.probability import ProbabilityEngine

def test_probability_heuristics():
    engine = ProbabilityEngine()
    
    # Test base heuristics
    edge1 = engine.evaluate_edge("POS_INIT__POS_INV", sequence_length=1)
    assert edge1.historical_win_rate == 0.62
    assert edge1.confidence == "LOW" # Sample size is 0
    
    # Test sequence maturity adjustment
    edge2 = engine.evaluate_edge("POS_INIT__POS_INV", sequence_length=3)
    assert edge2.historical_win_rate == 0.67 # 0.62 + 0.05
    
    edge3 = engine.evaluate_edge("POS_INIT__POS_INV", sequence_length=6)
    assert edge3.historical_win_rate == 0.57 # 0.62 - 0.05

def test_probability_dynamic_loading():
    engine = ProbabilityEngine()
    
    scorecard = [
        {
            "matrix_cell": "POS_INIT__POS_INV",
            "horizons": {
                "5m": {
                    "count": 50,
                    "win_rate": 0.8
                }
            }
        },
        {
            "matrix_cell": "NEG_INIT__NEG_INV",
            "horizons": {
                "5m": {
                    "count": 100,
                    "win_rate": 0.3
                }
            }
        }
    ]
    
    engine.refresh_stats(scorecard)
    
    # Test loaded stats
    edge1 = engine.evaluate_edge("POS_INIT__POS_INV", sequence_length=1)
    assert edge1.historical_win_rate == 0.80
    assert edge1.confidence == "MEDIUM" # 50 >= 20
    
    edge2 = engine.evaluate_edge("NEG_INIT__NEG_INV", sequence_length=1)
    assert edge2.historical_win_rate == 0.30
    assert edge2.confidence == "HIGH" # 100 >= 100
    
def test_edge_score_normalization():
    engine = ProbabilityEngine()
    
    # High WR, High RR
    scorecard = [
        {
            "matrix_cell": "POS_INIT__POS_INV",
            "horizons": {
                "5m": {
                    "count": 50,
                    "win_rate": 0.8
                }
            }
        }
    ]
    engine.refresh_stats(scorecard)
    edge = engine.evaluate_edge("POS_INIT__POS_INV", sequence_length=1)
    # WR=0.8, RR=1.5 (heuristic) -> Edge = (0.8 * 1.5) - 0.2 = 1.0
    # Normalization: (1.0 + 0.2) / 1.0 = 1.2 -> clamped to 1.0
    assert edge.edge_score == 1.0
