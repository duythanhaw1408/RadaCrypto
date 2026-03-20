import pytest
from cfte.models.events import TapeSnapshot, NormalizedTrade
from cfte.tpfm.engine import TPFMStateEngine

def test_liquidation_detection_flush():
    engine = TPFMStateEngine(symbol="BTCUSDT")
    
    # Create a snapshot with negative delta and high liquidation (Long Flush)
    snap = TapeSnapshot(
        instrument_key="binance:BTCUSDT:spot",
        window_start_ts=1000,
        window_end_ts=2000,
        spread_bps=1.0,
        microprice=50000.0,
        imbalance_l1=0.5,
        delta_quote=-100000.0, # Strong selling
        cvd=0.0,
        trade_burst=10.0,
        absorption_proxy=0.0,
        bid_px=49999.0,
        ask_px=50001.0,
        mid_px=50000.0,
        last_trade_px=49990.0,
        trade_count=100,
        futures_delta=-50000.0,
        liquidation_vol=150000.0 # High liquidation
    )
    
    # Empty trades for base calculation (just to satisfy method signature)
    trades = [NormalizedTrade("1", "binance", "instr", 50000.0, 1.0, 50000.0, "SELL", 1500)]
    
    futures_context = {
        "available": True,
        "fresh": True,
        "futures_delta": -50000.0,
        "liquidation_vol": 150000.0,
        "liquidation_bias": "LONGS_FLUSHED",
        "liquidation_context_available": True
    }
    
    tpfm_snap = engine.calculate_m5_snapshot(
        window_start_ts=1000,
        window_end_ts=2000,
        trades=trades,
        snapshots=[snap],
        futures_context=futures_context
    )
    
    assert tpfm_snap.liquidation_bias == "LONGS_FLUSHED"
    assert "LONG_FLUSH_DETECTED" in tpfm_snap.micro_conclusion
    assert any("Thanh lý lớn" in r for r in tpfm_snap.escalation_reason)
    assert any("flush" in f.lower() or "thanh lý" in f.lower() for f in tpfm_snap.observed_facts)

def test_venue_confirmation_logic():
    engine = TPFMStateEngine(symbol="BTCUSDT")
    
    # Test Confirmed state
    futures_context = {
        "available": True,
        "venue_confirmation_state": "CONFIRMED",
        "leader_venue": "binance"
    }
    
    snap = TapeSnapshot(
        instrument_key="binance:BTCUSDT:spot",
        window_start_ts=1000,
        window_end_ts=2000,
        spread_bps=1.0,
        microprice=50000.0,
        imbalance_l1=0.5,
        delta_quote=1000.0,
        cvd=0.0,
        trade_burst=1.0,
        absorption_proxy=0.0,
        bid_px=49999.0,
        ask_px=50001.0,
        mid_px=50000.0,
        last_trade_px=50000.0,
        trade_count=10,
    )
    
    trades = [NormalizedTrade("1", "binance", "instr", 50000.0, 1.0, 50000.0, "BUY", 1500)]
    
    tpfm_snap = engine.calculate_m5_snapshot(
        window_start_ts=1000,
        window_end_ts=2000,
        trades=trades,
        snapshots=[snap],
        futures_context=futures_context
    )
    
    assert tpfm_snap.venue_confirmation_state == "CONFIRMED"
    assert "NO_VENUE_CONFIRMATION" not in tpfm_snap.blind_spot_flags
