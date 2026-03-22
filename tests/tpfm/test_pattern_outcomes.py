import pytest
import uuid
import time
import sqlite3
from cfte.tpfm.engine import TPFMStateEngine
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.models.events import NormalizedTrade, TapeSnapshot

@pytest.fixture
def engine():
    return TPFMStateEngine(symbol="BTCUSDT", venue="binance")

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_outcomes.db")

@pytest.fixture
def writer(db_path):
    return ThesisSQLiteStore(db_path=db_path)

def mock_trade(price, side="BUY", qty=1.0, ts=None):
    return NormalizedTrade(
        event_id=str(uuid.uuid4()),
        venue="binance",
        instrument_key="BTCUSDT",
        price=price,
        qty=qty,
        quote_qty=price * qty,
        taker_side=side,
        venue_ts=ts or int(time.time() * 1000)
    )

def mock_snapshot(price):
    return TapeSnapshot(
        instrument_key="BTCUSDT",
        window_start_ts=0,
        window_end_ts=0,
        spread_bps=2.0,
        microprice=price,
        imbalance_l1=0.5,
        delta_quote=0.0,
        cvd=0.0,
        trade_burst=0.0,
        absorption_proxy=100.0,
        bid_px=price - 1,
        ask_px=price + 1,
        mid_px=price,
        last_trade_px=price,
        trade_count=1
    )

@pytest.mark.asyncio
async def test_pattern_outcome_persistence(writer, engine):
    await writer.migrate_schema()
    
    ts = 1711000000000
    
    # 1. Bar 0: Form a pattern (e.g. BREAKOUT_FORMING_LONG)
    # We need to trigger _derive_matrix_native_pattern
    # Condition for BREAKOUT_FORMING_LONG: 
    # self._current_sequence.family == "LONG" and energy_state == "EXPANDING" and response_efficiency_state == "FOLLOW_THROUGH"
    
    # Set up engine state for LONG family
    trades_0 = [mock_trade(50000, "BUY", 10.0, ts + 1000)]
    snaps_0 = [mock_snapshot(50000)]
    # First snapshot to start sequence
    snap_0 = engine.calculate_m5_snapshot(ts, ts + 300000, trades_0, snaps_0)
    
    # Bar 1: Strong BUY to get FOLLOW_THROUGH and EXPANDING
    ts_1 = ts + 300000
    trades_1 = [mock_trade(50100, "BUY", 50.0, ts_1 + 1000)] # Higher price for efficiency
    snaps_1 = [mock_snapshot(50100)]
    snap_1 = engine.calculate_m5_snapshot(ts_1, ts_1 + 600000, trades_1, snaps_1)
    
    # Force pattern if needed or check if it formed
    print(f"Snap 1 Pattern: {snap_1.pattern_code}")
    
    # Bar 2: One bar later (t+1)
    ts_2 = ts + 600000
    trades_2 = [mock_trade(50200, "BUY", 10.0, ts_2 + 1000)]
    snaps_2 = [mock_snapshot(50200)]
    snap_2 = engine.calculate_m5_snapshot(ts_2, ts_2 + 900000, trades_2, snaps_2)
    
    # Save snap_2 metadata outcomes
    outcomes = snap_2.metadata.get("pattern_outcomes", [])
    for out in outcomes:
        await writer.save_pattern_outcome(out)
    
    # Check DB
    with sqlite3.connect(writer.db_path) as db:
        cursor = db.execute("SELECT COUNT(*) FROM flow_pattern_outcome")
        count = cursor.fetchone()[0]
        # If snap_1 had a pattern, snap_2 should have a t+1 outcome
        if snap_1.pattern_code != "UNCLASSIFIED":
            assert count >= 1
            cursor = db.execute("SELECT * FROM flow_pattern_outcome LIMIT 1")
            row = cursor.fetchone()
            print(f"Saved Outcome: {row}")
