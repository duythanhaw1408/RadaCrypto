import json
from pathlib import Path

from cfte.cli.main import build_context, command_run_scan
from cfte.cli.runtime import LiveThesisLoop
from cfte.collectors.binance_public import BinancePublicCollector
from cfte.models.events import NormalizedDepthDiff, NormalizedTrade


def test_run_scan_persists_summary_and_thesis_log(tmp_path, capsys):
    profile_path = tmp_path / 'profile.yaml'
    profile_path.write_text(
        '\n'.join([
            'profile: test-personal',
            'locale: vi-VN',
            'trader:',
            '  display_name: Trader test',
            'defaults:',
            '  symbol: BTCUSDT',
            '  replay_events: fixtures/replay/btcusdt_normalized.jsonl',
            f'  summary_out: {tmp_path / "summary.json"}',
            'scan:',
            '  actionable_threshold: 75',
            '  max_cards: 2',
            f'  thesis_log: {tmp_path / "scan_thesis.jsonl"}',
            'live:',
            f'  thesis_log: {tmp_path / "live_thesis.jsonl"}',
            'review:',
            f'  summary_path: {tmp_path / "summary.json"}',
        ]),
        encoding='utf-8',
    )
    context = build_context(profile_path)

    exit_code = command_run_scan(context, events_path=Path('fixtures/replay/btcusdt_normalized.jsonl'), limit=None)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert 'Đã lưu summary scan tại' in captured.out
    assert 'Đã ghi log thesis scan tại' in captured.out

    summary = json.loads((tmp_path / 'summary.json').read_text(encoding='utf-8'))
    thesis_records = (tmp_path / 'scan_thesis.jsonl').read_text(encoding='utf-8').strip().splitlines()
    thesis_payload = json.loads(thesis_records[0])

    assert summary['instrument_key'] == 'BINANCE:BTCUSDT:SPOT'
    assert thesis_payload['flow'] == 'scan'
    assert thesis_payload['selected_count'] >= 1
    assert thesis_payload['signals'][0]['setup']


def test_live_thesis_loop_generates_ranked_signals_from_trade_window():
    loop = LiveThesisLoop(instrument_key='BINANCE:BTCUSDT:SPOT', trade_window_size=2)
    loop.apply_snapshot(bids=[(100.0, 10.0)], asks=[(100.1, 2.0)], seq_id=1)
    loop.ingest_depth(
        NormalizedDepthDiff(
            event_id='d1',
            venue='binance',
            instrument_key='BINANCE:BTCUSDT:SPOT',
            first_update_id=2,
            final_update_id=2,
            bid_updates=[(100.0, 12.0)],
            ask_updates=[(100.1, 1.5)],
            venue_ts=1_000,
        )
    )

    first = loop.ingest_trade(
        NormalizedTrade(
            event_id='t1',
            venue='binance',
            instrument_key='BINANCE:BTCUSDT:SPOT',
            price=100.1,
            qty=1.0,
            quote_qty=100.1,
            taker_side='BUY',
            venue_ts=2_000,
        )
    )
    second = loop.ingest_trade(
        NormalizedTrade(
            event_id='t2',
            venue='binance',
            instrument_key='BINANCE:BTCUSDT:SPOT',
            price=100.12,
            qty=1.2,
            quote_qty=120.144,
            taker_side='BUY',
            venue_ts=3_000,
        )
    )

    assert first.event_type == 'trade'
    assert len(first.signals) == 4
    assert len(loop.trades) == 2
    assert second.signals[0].score >= second.signals[-1].score
    assert second.signals[0].instrument_key == 'BINANCE:BTCUSDT:SPOT'


def test_binance_health_snapshot_reports_degraded_state():
    collector = BinancePublicCollector(streams=['btcusdt@aggTrade'])

    collector._record_failure(ConnectionError('ws reset by peer'))
    snapshot = collector.health_snapshot()

    assert snapshot.venue == 'binance'
    assert snapshot.state == 'degraded'
    assert snapshot.connected is False
    assert snapshot.reconnect_count == 1
    assert snapshot.last_error is not None
    assert 'ws reset by peer' in snapshot.to_operator_summary()


def test_live_thesis_loop_persists_runtime_artifact_on_watchdog_timeout(tmp_path):
    import asyncio
    import json
    from unittest.mock import patch
    from cfte.live.engine import LiveThesisLoop as _LiveThesisLoop

    runtime_path = tmp_path / 'live_runtime.json'
    db_path = tmp_path / 'state.db'
    import sqlite3
    conn = sqlite3.connect(db_path)
    for sql_name in ['001_state.sql', '002_indexes.sql']:
        conn.executescript((Path('sql/sqlite') / sql_name).read_text(encoding='utf-8'))
    conn.commit()
    conn.close()

    class _SilentCollector:
        def __init__(self, streams):
            self._message_count = 0
        async def stream_forever(self):
            while True:
                await asyncio.sleep(0.05)
                if False:
                    yield {}
        def health_snapshot(self):
            from cfte.collectors.health import CollectorHealthSnapshot
            return CollectorHealthSnapshot('binance', 'running', True, 1, 0, self._message_count, None, None)

    live = _LiveThesisLoop(
        symbol='BTCUSDT',
        db_path=db_path,
        runtime_report_path=runtime_path,
        watchdog_idle_seconds=0.01,
        heartbeat_interval=1,
        max_retries=1,
    )

    with patch('cfte.live.engine.try_fetch_depth_snapshot', return_value=({'lastUpdateId': 1, 'bids': [['100', '1']], 'asks': [['101', '1']]}, None)):
        with patch('cfte.live.engine.BinancePublicCollector', _SilentCollector):
            asyncio.run(live.run_forever(max_events=1))

    artifact = json.loads(runtime_path.read_text(encoding='utf-8'))
    assert artifact['status'] == 'watchdog_timeout'
    assert artifact['idle_timeout_seconds'] == 0.01
    assert 'Watchdog' in artifact['last_error']
    assert artifact['run_id']
    assert artifact['pid'] is not None
    assert artifact['lock_path']
    assert Path(artifact['lock_path']).exists() is False
