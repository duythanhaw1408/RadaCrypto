import pytest
import uuid
import time
import json
import os
import sqlite3
from datetime import datetime, timezone
from cfte.tpfm.engine import TPFMStateEngine
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.models.events import NormalizedTrade, TapeSnapshot

@pytest.fixture
def engine():
    return TPFMStateEngine(symbol="BTCUSDT", venue="binance")

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_phase2.db")

@pytest.fixture
def writer(db_path):
    return ThesisSQLiteStore(db_path=db_path)

def mock_trade(price, side="BUY", qty=1.0):
    return NormalizedTrade(
        event_id=str(uuid.uuid4()),
        venue="binance",
        instrument_key="BTCUSDT",
        price=price,
        qty=qty,
        quote_qty=price * qty,
        taker_side=side,
        venue_ts=int(time.time() * 1000)
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
async def test_sequence_pivot_and_duration(engine):
    ts_start = 1000000
    
    # 1. First Snapshot (Start Sequence)
    trades_1 = [mock_trade(50000, "BUY", 2.0)]
    snaps_1 = [mock_snapshot(50000)]
    snap1 = engine.calculate_m5_snapshot(ts_start, ts_start + 300000, trades_1, snaps_1)
    
    assert snap1.is_sequence_pivot is True
    assert snap1.sequence_length == 1
    assert snap1.sequence_duration_sec == 300.0
    
    # 2. Second Snapshot (Continue Sequence)
    trades_2 = [mock_trade(50010, "BUY", 2.0)]
    snaps_2 = [mock_snapshot(50010)]
    snap2 = engine.calculate_m5_snapshot(ts_start + 300000, ts_start + 600000, trades_2, snaps_2)
    
    assert snap2.is_sequence_pivot is False
    assert snap2.sequence_length == 2
    assert snap2.sequence_duration_sec == 600.0
    
    # 3. Third Snapshot (Break/Pivot Sequence) - Flip to SELL
    # We need a big move to change the matrix_cell
    trades_3 = [mock_trade(49000, "SELL", 50.0)]
    snaps_3 = [mock_snapshot(49000)]
    snap3 = engine.calculate_m5_snapshot(ts_start + 600000, ts_start + 900000, trades_3, snaps_3)
    
    if snap3.matrix_cell != snap2.matrix_cell:
        assert snap3.is_sequence_pivot is True
        assert snap3.sequence_length == 1
        assert snap3.sequence_duration_sec == 300.0

@pytest.mark.asyncio
async def test_mtf_and_outcome_presence(engine):
    trades = [mock_trade(50000)]
    snaps = [mock_snapshot(50000)]
    snap = engine.calculate_m5_snapshot(1000, 2000, trades, snaps)
    
    assert snap.parent_context is not None
    assert "m30_regime" in snap.parent_context
    assert snap.t_plus_1_price > 0
    assert snap.t_plus_5_price > 0
    assert snap.t_plus_12_price > 0

@pytest.mark.asyncio
async def test_db_persistence_phase2(writer, engine):
    await writer.migrate_schema()
    
    trades = [mock_trade(50000)]
    snaps = [mock_snapshot(50000)]
    snap = engine.calculate_m5_snapshot(1000, 2000, trades, snaps)
    
    await writer.save_tpfm_snapshot(snap)
    
    # Verify columns exist in DB
    with sqlite3.connect(writer.db_path) as db:
        cursor = db.execute("SELECT * FROM tpfm_m5_snapshot LIMIT 1")
        col_names = [description[0] for description in cursor.description]
        
        assert "sequence_start_ts" in col_names
        assert "is_sequence_pivot" in col_names
        assert "t_plus_5_price" in col_names
        
        row = cursor.fetchone()
        row_dict = dict(zip(col_names, row))
        assert row_dict["sequence_start_ts"] == snap.sequence_start_ts
        assert row_dict["is_sequence_pivot"] == int(snap.is_sequence_pivot)
