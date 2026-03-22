import pytest
from unittest.mock import AsyncMock, MagicMock
from cfte.tpfm.engine import TPFMStateEngine

@pytest.mark.asyncio
async def test_probability_db_sync():
    engine = TPFMStateEngine(symbol="BTCUSDT", venue="binance")
    
    # Mock DB writer
    mock_db = MagicMock()
    mock_scorecard = [
        {
            "matrix_cell": "POS_INIT__POS_INV",
            "horizons": {
                "5m": {
                    "count": 50,
                    "wins": 40,
                    "win_rate": 0.8,
                    "avg_edge": 0.5
                }
            }
        },
        {
            "matrix_cell": "NEG_INIT__NEG_INV",
            "horizons": {
                "5m": {
                    "count": 100,
                    "wins": 30,
                    "win_rate": 0.3,
                    "avg_edge": -0.2
                }
            }
        }
    ]
    mock_db.get_matrix_scorecard = AsyncMock(return_value=mock_scorecard)
    
    # Trigger sync
    success = await engine.sync_probability_stats(mock_db)
    assert success is True
    
    # Verify stats in ProbabilityEngine
    # POS_INIT__POS_INV should have WR 0.8
    edge1 = engine._probability_engine.evaluate_edge("POS_INIT__POS_INV")
    assert edge1.historical_win_rate == 0.8
    assert edge1.confidence == "MEDIUM" # 50 >= 20
    
    # NEG_INIT__NEG_INV should have WR 0.3
    edge2 = engine._probability_engine.evaluate_edge("NEG_INIT__NEG_INV")
    assert edge2.historical_win_rate == 0.3
    assert edge2.confidence == "HIGH" # 100 >= 100
    
    # Test fallback horizon
    mock_scorecard_fallback = [
        {
            "matrix_cell": "POS_INIT__NEG_INV",
            "horizons": {
                "15m": {
                    "count": 10,
                    "wins": 7,
                    "win_rate": 0.7,
                    "avg_edge": 0.1
                }
            }
        }
    ]
    mock_db.get_matrix_scorecard = AsyncMock(return_value=mock_scorecard_fallback)
    await engine.sync_probability_stats(mock_db)
    
    edge3 = engine._probability_engine.evaluate_edge("POS_INIT__NEG_INV")
    assert edge3.historical_win_rate == 0.7
    assert edge3.confidence == "LOW" # 10 < 20
