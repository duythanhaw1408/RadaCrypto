import pytest
import json
from pathlib import Path
from cfte.tpfm.engine import TPFMStateEngine
from cfte.models.events import NormalizedTrade
from cfte.features.tape import build_tape_snapshot, TapeSnapshot
from cfte.storage.sqlite_writer import ThesisSQLiteStore

def test_tpfm_vnext_metrics_and_transitions():
    engine = TPFMStateEngine(symbol="BTCUSDT")
    
    # Mock trades for first M5
    trades_1 = [
        NormalizedTrade(
            event_id="t1",
            venue="binance",
            instrument_key="BTCUSDT",
            price=50000.0,
            qty=1.0,
            quote_qty=50000.0,
            taker_side="BUY",
            venue_ts=1700000000000
        )
    ]
    snapshots_1 = [
        TapeSnapshot(
            instrument_key="BTCUSDT",
            window_start_ts=1700000000000,
            window_end_ts=1700000000000,
            spread_bps=1.0,
            microprice=50000.0,
            imbalance_l1=0.5,
            delta_quote=50000.0,
            cvd=50000.0,
            trade_burst=1.0,
            absorption_proxy=0.0,
            bid_px=49999.0,
            ask_px=50001.0,
            mid_px=50000.0,
            last_trade_px=50000.0,
            trade_count=1
        )
    ]
    
    snap1, trans1 = engine.calculate_m5_snapshot(
        window_start_ts=1700000000000,
        window_end_ts=1700000300000,
        trades=trades_1,
        snapshots=snapshots_1
    )
    
    assert snap1.matrix_cell != "UNKNOWN"
    assert trans1 is None # First snapshot, no transition
    assert snap1.aggression_ratio == 1.0
    
    # Mock trades for second M5 (Shift to selling)
    trades_2 = [
        NormalizedTrade(
            event_id="t2",
            venue="binance",
            instrument_key="BTCUSDT",
            price=49900.0,
            qty=2.0,
            quote_qty=99800.0,
            taker_side="SELL",
            venue_ts=1700000300000 + 1
        )
    ]
    snapshots_2 = [
        TapeSnapshot(
            instrument_key="BTCUSDT",
            window_start_ts=1700000300000 + 1,
            window_end_ts=1700000300000 + 1,
            spread_bps=1.0,
            microprice=49900.0,
            imbalance_l1=0.5,
            delta_quote=-99800.0,
            cvd=-49800.0,
            trade_burst=1.0,
            absorption_proxy=0.0,
            bid_px=49899.0,
            ask_px=49901.0,
            mid_px=49900.0,
            last_trade_px=49900.0,
            trade_count=1
        )
    ]
    
    snap2, trans2 = engine.calculate_m5_snapshot(
        window_start_ts=1700000300000,
        window_end_ts=1700000600000,
        trades=trades_2,
        snapshots=snapshots_2
    )
    
    assert snap2.matrix_cell != snap1.matrix_cell
    assert trans2 is not None
    assert trans2.from_cell == snap1.matrix_cell
    assert trans2.to_cell == snap2.matrix_cell
    assert "TO" in trans2.transition_code
    assert trans2.transition_family
    assert trans2.transition_alias_vi
    assert 0.0 <= trans2.transition_speed <= 1.0
    assert 0.0 <= trans2.transition_quality <= 1.0

def test_sqlite_vnext_schema(tmp_path):
    db_path = tmp_path / "test_vnext.db"
    store = ThesisSQLiteStore(db_path)
    # This should not raise
    import asyncio
    asyncio.run(store.migrate_schema())
    
    # Check if new columns exist
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        cols = [c[1] for c in conn.execute("PRAGMA table_info(tpfm_m5_snapshot)").fetchall()]
        assert "delta_zscore" in cols
        assert "aggression_ratio" in cols
        assert "sweep_quote" in cols
        assert "sweep_buy_quote" in cols
        assert "burst_persistence" in cols
        assert "basis_state" in cols
