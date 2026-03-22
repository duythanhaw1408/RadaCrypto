import pytest
from unittest.mock import MagicMock
from cfte.tpfm.engine import TPFMStateEngine
from cfte.models.events import NormalizedTrade, TapeSnapshot
from cfte.tpfm.models import TPFMSnapshot

@pytest.mark.asyncio
async def test_ai_brief_generation():
    engine = TPFMStateEngine()
    # Mock the AI explainer to avoid real API calls
    engine._ai_explainer.explain_m5_brief = MagicMock(return_value="Mocked AI Brief: Long bias confirmed.")
    
    # Corrected Mock data
    trade = NormalizedTrade(
        event_id="t1", 
        venue="binance",
        instrument_key="BTCUSDT", 
        price=50000.0, 
        qty=0.1, 
        quote_qty=5000.0, 
        taker_side="BUY",
        venue_ts=1000
    )
    
    snap = TapeSnapshot(
        instrument_key="BTCUSDT",
        window_start_ts=900,
        window_end_ts=1100,
        spread_bps=0.4,
        microprice=50000.2,
        imbalance_l1=0.1,
        delta_quote=1000.0,
        cvd=5000.0,
        trade_burst=2.5,
        absorption_proxy=0.05,
        bid_px=49999.0,
        ask_px=50001.0,
        mid_px=50000.0,
        last_trade_px=50000.5,
        trade_count=10
    )
    
    # Calculate with use_ai_brief=True
    tpfm_snap = engine.calculate_m5_snapshot(
        900, 1100, [trade], [snap], use_ai_brief=True
    )
    
    assert tpfm_snap.flow_decision_brief == "Mocked AI Brief: Long bias confirmed."
    engine._ai_explainer.explain_m5_brief.assert_called_once()
