import json
import sqlite3
from pathlib import Path

from cfte.cli.main import build_context, command_log_review, command_review_day, command_review_week, command_tune_profile
from cfte.models.events import ThesisSignal
from cfte.storage.measurement import render_daily_summary_vi
from cfte.storage.review_journal import build_tuning_suggestions, summarize_review_journal
from cfte.storage.sqlite_writer import ThesisSQLiteStore


def _write_profile(tmp_path: Path) -> Path:
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(
        "\n".join([
            "profile: test-operate",
            "locale: vi-VN",
            "trader:",
            "  display_name: Trader tune",
            "defaults:",
            "  symbol: BTCUSDT",
            "  replay_events: fixtures/replay/btcusdt_normalized.jsonl",
            f"  summary_out: {tmp_path / 'summary.json'}",
            "scan:",
            "  actionable_threshold: 74",
            "review:",
            f"  summary_path: {tmp_path / 'summary.json'}",
            f"  daily_summary_path: {tmp_path / 'daily_summary.json'}",
            f"  weekly_summary_path: {tmp_path / 'weekly_review.json'}",
            f"  review_journal_path: {tmp_path / 'review_journal.jsonl'}",
            f"  tuning_report_path: {tmp_path / 'tuning_report.json'}",
        ]),
        encoding="utf-8",
    )
    return profile_path


def _seed_thesis(db_path: Path, thesis_id: str = "thesis-1") -> None:
    import asyncio

    store = ThesisSQLiteStore(db_path)

    async def _run() -> None:
        await store.migrate_schema()
        signal = ThesisSignal(
            thesis_id=thesis_id,
            instrument_key="BINANCE:BTCUSDT:SPOT",
            setup="breakout_ignition",
            direction="LONG_BIAS",
            stage="ACTIONABLE",
            score=82.0,
            confidence=0.71,
            coverage=0.8,
            why_now=["burst"],
            conflicts=[],
            invalidation="< 99",
            entry_style="pullback",
            targets=["102"],
        )
        await store.save_thesis(signal, opened_ts=1_710_720_000_000, entry_px=100.0)
        await store.init_outcomes(signal.thesis_id, ["24h"], opened_ts=1_710_720_000_000)
        await store.save_outcome(signal.thesis_id, "24h", realized_px=103.0, realized_high=104.0, realized_low=99.5)
        await store.finalize_thesis_from_outcome(signal.thesis_id, "24h", updated_at=1_710_806_400_000)

    asyncio.run(_run())


def test_log_review_and_daily_weekly_outputs(tmp_path, capsys):
    profile_path = _write_profile(tmp_path)
    context = build_context(profile_path)

    state_dir = Path('data/state')
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / 'state.db'
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    for sql_name in ["001_state.sql", "002_indexes.sql"]:
        conn.executescript((Path("sql/sqlite") / sql_name).read_text(encoding="utf-8"))
    conn.commit()
    conn.close()
    _seed_thesis(db_path)

    rc = command_log_review(context, thesis_id='thesis-1', decision='taken', usefulness='useful', note='đúng plan', review_ts='2024-03-11T08:00:00')
    assert rc == 0
    command_review_day(context, date_str='2024-03-11', summary_path=tmp_path / 'summary.json')
    command_review_week(context, end_date_str='2024-03-11')
    command_tune_profile(context)
    out = capsys.readouterr().out

    assert 'Đã ghi review cá nhân' in out
    assert 'Nhật ký review cá nhân' in out
    assert 'Gợi ý tuning threshold cá nhân' in out
    daily = json.loads((tmp_path / 'daily_summary.json').read_text(encoding='utf-8'))
    weekly = json.loads((tmp_path / 'weekly_review.json').read_text(encoding='utf-8'))
    tuning = json.loads((tmp_path / 'tuning_report.json').read_text(encoding='utf-8'))
    assert daily['review_summary']['total_reviews'] == 1
    assert weekly['review_summary']['decision_counts']['taken'] == 1
    assert tuning['tuning_suggestions'][0]['setup'] == 'breakout_ignition'


def test_render_daily_summary_includes_review_counts():
    stats = {
        'label': '2026-03-18',
        'opened_count': 2,
        'avg_score': 80.0,
        'avg_confidence': 0.7,
        'outcomes_count': 1,
        'positive_outcomes': 1,
        'avg_edge': 1.2,
        'setup_dist': {'breakout_ignition': 2},
        'stage_dist': {'ACTIONABLE': 2},
        'closed_stage_dist': {'RESOLVED': 1},
    }
    text = render_daily_summary_vi(stats, {'decision_counts': {'taken': 1}, 'usefulness_counts': {'useful': 1}})
    assert 'Review cá nhân' in text
    assert 'hữu ích=1' in text


def test_build_tuning_suggestions_tightens_noisy_setup():
    scorecard = [{
        'setup': 'distribution',
        'total_signals': 5,
        'resolved_count': 1,
        'invalidated_count': 3,
        'horizons': {'24h': {'avg_edge': -0.8, 'win_rate': 0.2, 'count': 5}},
    }]
    review_summary = summarize_review_journal([
        {'setup': 'distribution', 'decision': 'ignored', 'usefulness': 'noise', 'review_ts': 1},
        {'setup': 'distribution', 'decision': 'skipped', 'usefulness': 'noise', 'review_ts': 2},
    ])
    suggestions = build_tuning_suggestions(scorecard, review_summary, base_threshold=70.0)
    assert suggestions[0]['suggested_threshold'] > 70.0
    assert 'nhiễu' in suggestions[0]['rationale_vi'][0]
