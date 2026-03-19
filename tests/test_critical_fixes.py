import pytest
import ssl
import certifi
import json
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone
from pathlib import Path

from cfte.live.engine import LiveThesisLoop
from cfte.models.events import NormalizedTrade, TapeSnapshot, ThesisSignal
from cfte.thesis.state import ThesisEventRecord, ThesisLifecycleRecord
from cfte.storage.sqlite_writer import ThesisSQLiteStore

@pytest.mark.asyncio
async def test_live_engine_nameerror_fix(tmp_path):
    # Setup mock store and engine
    db_path = tmp_path / "test.db"
    store = MagicMock(spec=ThesisSQLiteStore)
    store.save_thesis = AsyncMock()
    store.init_outcomes = AsyncMock()
    store.append_event = AsyncMock()
    
    engine = LiveThesisLoop(
        symbol="BTCUSDT",
        db_path=db_path,
        runtime_report_path=tmp_path / "runtime.json",
    )
    engine.store = store
    engine.thesis_log = MagicMock()
    
    # Mock data
    trade = NormalizedTrade(
        event_id="t1", venue="binance", instrument_key="BINANCE:BTCUSDT:SPOT",
        price=50000.0, qty=1.0, quote_qty=50000.0, taker_side="BUY", venue_ts=1000
    )
    snapshot = TapeSnapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT", 
        window_start_ts=1000,
        window_end_ts=1000,
        bid_px=49999.0, ask_px=50001.0, mid_px=50000.0, last_trade_px=50000.0,
        microprice=50000.0, delta_quote=100.0, imbalance_l1=0.6,
        trade_count=10, trade_burst=1.5, spread_bps=2.0, absorption_proxy=20.0,
        cvd=0.0
    )
    
    signal = ThesisSignal(
        thesis_id="id1", instrument_key="BINANCE:BTCUSDT:SPOT", setup="stealth_accumulation",
        direction="LONG_BIAS", stage="DETECTED", score=65.0, confidence=0.7,
        coverage=0.8, why_now=[], conflicts=[], invalidation="", entry_style="",
        targets=[], timeframe="1h", regime_bucket="NEUTRAL"
    )
    
    event1 = ThesisEventRecord(
        thesis_id="id1", event_type="stage_transition", from_stage="DETECTED",
        to_stage="WATCHLIST", event_ts=1000, summary_vi="Test Event 1",
        score=65.0, confidence=0.7
    )
    event2 = ThesisEventRecord(
        thesis_id="id1", event_type="stage_transition", from_stage="WATCHLIST",
        to_stage="CONFIRMED", event_ts=1000, summary_vi="Test Event 2",
        score=70.0, confidence=0.8
    )
    
    next_state = ThesisLifecycleRecord(signal=signal, opened_ts=1000, updated_ts=1000)
    
    # Mock external functions
    with patch("cfte.live.engine.evaluate_setups", return_value=[signal]):
        with patch("cfte.live.engine.apply_signal_update", return_value=(next_state, [event1, event2])):
            with patch("cfte.live.engine.build_tape_snapshot", return_value=snapshot):
                # Setup engine internal state to bypass early returns
                engine._depth = MagicMock()
                engine._depth.book.bids = [(49999.0, 1.0)]
                engine._depth.book.asks = [(50001.0, 1.0)]
                
                # Use a real list for trades
                engine._trades = [trade]
                engine._tpfm_trades = []
                engine._tpfm_window_start_ts = 0
                
                # Trigger the private _process_trade_event
                await engine._process_trade_event(trade)
                
                # Verify MULTIPLE events were appended (fixing the NameError which only used 'event')
                assert store.append_event.call_count == 2
                store.append_event.assert_any_call(event1)
                store.append_event.assert_any_call(event2)
                
                # Verify thesis log calls
                assert engine.thesis_log.append_record.call_count == 2

def test_binance_collector_secure_tls():
    from cfte.collectors.binance_public import BinancePublicCollector
    import ssl
    
    with patch("ssl.create_default_context") as mock_ssl:
        with patch("certifi.where", return_value="/fake/path/cert.pem"):
            collector = BinancePublicCollector(streams=["btcusdt@aggTrade"])
            
            with patch("websockets.connect", side_effect=Exception("stop")):
                import asyncio
                try:
                    asyncio.run(collector.stream_forever().__anext__())
                except:
                    pass
            
            # Verify ssl.create_default_context was called with cafile=certifi.where()
            mock_ssl.assert_called_with(cafile="/fake/path/cert.pem")

@pytest.mark.asyncio
async def test_sqlite_writer_utc_consistency(tmp_path):
    db_path = tmp_path / "state.db"
    store = ThesisSQLiteStore(db_path)
    
    fixed_now = datetime(2026, 3, 19, 1, 0, 0, tzinfo=timezone.utc)
    with patch("cfte.storage.sqlite_writer.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.strptime.side_effect = datetime.strptime
        
        with patch("cfte.storage.sqlite_writer.sqlite3.connect"):
            store.get_period_summary = AsyncMock()
            
            await store.get_daily_summary_stats()
            
            store.get_period_summary.assert_called_once()
            args, kwargs = store.get_period_summary.call_args
            assert kwargs['label'] == "2026-03-19"
            
            expected_start = int(datetime(2026, 3, 19, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
            assert kwargs['start_ts'] == expected_start
