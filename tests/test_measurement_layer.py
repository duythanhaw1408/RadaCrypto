import asyncio
import json
import sqlite3
from pathlib import Path

from cfte.cli.main import build_context, command_review_day, command_review_week, command_scorecard
from cfte.models.events import ThesisSignal
from cfte.storage.sqlite_writer import ThesisSQLiteStore

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript((ROOT / 'sql/sqlite/001_state.sql').read_text(encoding='utf-8'))
    conn.executescript((ROOT / 'sql/sqlite/002_indexes.sql').read_text(encoding='utf-8'))
    conn.close()


def test_store_finalizes_thesis_and_builds_scorecard(tmp_path):
    db_path = tmp_path / 'state.db'
    _bootstrap_db(db_path)
    store = ThesisSQLiteStore(db_path)

    signal = ThesisSignal(
        thesis_id='BINANCE:BTCUSDT:SPOT|stealth_accumulation|LONG_BIAS|1h|NEUTRAL',
        instrument_key='BINANCE:BTCUSDT:SPOT',
        setup='stealth_accumulation',
        direction='LONG_BIAS',
        stage='ACTIONABLE',
        score=82.0,
        confidence=76.0,
        coverage=68.0,
        why_now=['test'],
        conflicts=[],
        invalidation='below local bid wall',
        entry_style='passive',
        targets=['1h'],
    )

    async def _run():
        await store.migrate_schema()
        await store.save_thesis(signal, opened_ts=1_000, entry_px=100.0)
        await store.init_outcomes(signal.thesis_id, ['1h', '24h'], opened_ts=1_000)
        await store.save_outcome(signal.thesis_id, '1h', realized_px=101.0, realized_high=102.0, realized_low=99.5)
        await store.save_outcome(signal.thesis_id, '24h', realized_px=104.0, realized_high=105.0, realized_low=99.0)
        stage = await store.finalize_thesis_from_outcome(signal.thesis_id, '24h', updated_at=90_000)
        thesis = await store.get_thesis_by_id(signal.thesis_id)
        scorecard = await store.get_setup_scorecard()
        return stage, thesis, scorecard

    stage, thesis, scorecard = asyncio.run(_run())

    assert stage == 'RESOLVED'
    assert thesis is not None
    assert thesis['stage'] == 'RESOLVED'
    assert thesis['closed_ts'] == 90_000
    assert scorecard[0]['setup'] == 'stealth_accumulation'
    assert scorecard[0]['horizons']['24h']['avg_edge'] > 0


def test_review_commands_emit_vietnamese_summaries_and_files(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / 'state.db'
    _bootstrap_db(db_path)

    profile_path = tmp_path / 'profile.yaml'
    profile_path.write_text(
        '\n'.join([
            'profile: measurement-test',
            'locale: vi-VN',
            'trader:',
            '  display_name: Trader đo lường',
            'defaults:',
            '  symbol: BTCUSDT',
            '  replay_events: fixtures/replay/btcusdt_normalized.jsonl',
            f'  summary_out: {tmp_path / "summary.json"}',
            'scan:',
            f'  thesis_log: {tmp_path / "scan.jsonl"}',
            'live:',
            f'  thesis_log: {tmp_path / "live.jsonl"}',
            'review:',
            f'  daily_summary_path: {tmp_path / "daily_summary.json"}',
            f'  weekly_summary_path: {tmp_path / "weekly_review.json"}',
        ]),
        encoding='utf-8',
    )
    context = build_context(profile_path)

    async def _seed():
        store = ThesisSQLiteStore(db_path)
        await store.migrate_schema()
        signal = ThesisSignal(
            thesis_id='t1',
            instrument_key='BINANCE:BTCUSDT:SPOT',
            setup='distribution',
            direction='SHORT_BIAS',
            stage='ACTIONABLE',
            score=78.0,
            confidence=71.0,
            coverage=65.0,
            why_now=['test'],
            conflicts=[],
            invalidation='above local ask wall',
            entry_style='aggressive',
            targets=['1h', '24h'],
        )
        opened_ts = 1_710_720_000_000  # 2024-03-11 UTC
        await store.save_thesis(signal, opened_ts=opened_ts, entry_px=100.0)
        await store.init_outcomes('t1', ['24h'], opened_ts=opened_ts)
        await store.save_outcome('t1', '24h', realized_px=95.0, realized_high=101.0, realized_low=94.0)
        await store.finalize_thesis_from_outcome('t1', '24h', updated_at=opened_ts + 86_400_000)

    asyncio.run(_seed())

    import cfte.cli.main as cli_main
    monkeypatch.setattr(cli_main, 'DEFAULT_STATE_DB', db_path)

    assert command_review_day(context, date_str='2024-03-11') == 0
    day_out = capsys.readouterr().out
    assert 'Tổng kết ngày 2024-03-11' in day_out
    assert 'Đã lưu daily summary tại' in day_out

    assert command_review_week(context, end_date_str='2024-03-17') == 0
    week_out = capsys.readouterr().out
    assert 'Review tuần' in week_out
    assert 'Bảng điểm setup' in week_out

    assert command_scorecard(context) == 0
    score_out = capsys.readouterr().out
    assert 'distribution' in score_out

    daily_payload = json.loads((tmp_path / 'daily_summary.json').read_text(encoding='utf-8'))
    weekly_payload = json.loads((tmp_path / 'weekly_review.json').read_text(encoding='utf-8'))
    assert daily_payload['summary_vi'].startswith('Tổng kết ngày')
    assert weekly_payload['summary_vi'].startswith('Review tuần')
