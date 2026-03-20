import pytest
from datetime import datetime, timezone
from cfte.collectors.binance_futures import BinanceFuturesCollector

def test_futures_collector_stale_detection():
    collector = BinanceFuturesCollector(symbol="BTCUSDT", context_window_seconds=10.0)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Initially not connected, no messages
    report = collector.get_health_report(now_ms=now_ms)
    assert report["connected"] is False
    assert report["is_stale"] is False # No messages yet, not stale by default
    
    # Simulate a message just received
    collector._last_ws_message_ts = now_ms
    collector._last_agg_trade_ts = now_ms
    collector._stream_connected = True
    
    report = collector.get_health_report(now_ms=now_ms)
    assert report["is_stale"] is False
    assert report["ws_latency_ms"] == 0
    
    # Simulate 20 seconds passing (greater than 15s threshold)
    future_ms = now_ms + 20000
    report = collector.get_health_report(now_ms=future_ms)
    assert report["is_stale"] is True
    assert report["ws_latency_ms"] == 20000

def test_futures_collector_stale_by_trade_gap():
    # window = 10s. stale if trade_age > 20s
    collector = BinanceFuturesCollector(symbol="BTCUSDT", context_window_seconds=10.0)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    collector._last_ws_message_ts = now_ms # WS is alive (e.g. heartbeat)
    collector._last_agg_trade_ts = now_ms - 25000 # but no trade for 25s
    collector._stream_connected = True
    
    report = collector.get_health_report(now_ms=now_ms)
    assert report["ws_latency_ms"] == 0
    assert report["agg_trade_age_ms"] == 25000
    assert report["is_stale"] is True

def test_live_context_freshness_integration():
    collector = BinanceFuturesCollector(symbol="BTCUSDT", context_window_seconds=10.0)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    collector._rest_context_cache = {
        "available": True,
        "fresh": True,
        "timestamp": now_ms,
        "mark_price": 70000.0,
        "index_price": 69990.0,
        "basis_bps": 1.43,
        "funding_rate": 0.0001,
        "oi_value": 1000.0,
        "oi_delta": 5.0,
    }
    collector._rest_context_ts = now_ms
    
    # WS alive but stale
    collector._last_ws_message_ts = now_ms - 20000
    ctx = collector.get_live_context(now_ms=now_ms)
    
    # Even if REST is fresh, global 'fresh' should be false if WS is too old
    # Note: get_live_context uses context_window_seconds (10s) as threshold
    assert ctx["fresh"] is False


def test_futures_health_snapshot_exposes_idle_gap_and_notes():
    collector = BinanceFuturesCollector(symbol="BTCUSDT", context_window_seconds=10.0)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    collector._state = "running"
    collector._connected = True
    collector._stream_connected = True
    collector._connect_attempts = 2
    collector._reconnect_count = 1
    collector._message_count = 8
    collector._last_ws_message_ts = now_ms - 3_000
    collector._last_agg_trade_ts = now_ms - 2_000
    collector._last_force_order_ts = now_ms - 1_000

    snapshot = collector.health_snapshot(now_ms=now_ms)

    assert snapshot.venue == "binance_futures"
    assert snapshot.connected is True
    assert snapshot.is_stale is False
    assert snapshot.idle_gap_seconds == 3.0
    assert any("agg_trade_age=2000ms" in note for note in snapshot.notes)
    assert "idle_gap=3.0s" in snapshot.to_operator_summary()
