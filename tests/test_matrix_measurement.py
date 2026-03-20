import asyncio
import sqlite3
from pathlib import Path

from cfte.models.events import ThesisSignal
from cfte.storage.measurement import (
    render_daily_summary_vi,
    render_flow_state_scorecard_vi,
    render_forced_flow_scorecard_vi,
    render_matrix_scorecard_vi,
    render_transition_scorecard_vi,
    render_weekly_review_vi,
)
from cfte.storage.sqlite_writer import ThesisSQLiteStore
from cfte.tpfm.models import TPFMSnapshot

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript((ROOT / "sql/sqlite/001_state.sql").read_text(encoding="utf-8"))
    conn.executescript((ROOT / "sql/sqlite/002_indexes.sql").read_text(encoding="utf-8"))
    conn.close()


def test_store_builds_matrix_scorecard_from_tpfm_snapshot(tmp_path):
    db_path = tmp_path / "state.db"
    _bootstrap_db(db_path)
    store = ThesisSQLiteStore(db_path)

    signal = ThesisSignal(
        thesis_id="thesis-matrix-1",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=84.0,
        confidence=0.78,
        coverage=0.82,
        why_now=["test"],
        conflicts=[],
        invalidation="below bid",
        entry_style="pullback",
        targets=["1h", "24h"],
    )

    async def _run():
        await store.migrate_schema()
        await store.save_tpfm_snapshot(
            TPFMSnapshot(
                snapshot_id="snap-matrix-1",
                symbol="BTCUSDT",
                window_start_ts=900,
                window_end_ts=1_100,
                matrix_cell="POS_INIT__POS_INV",
                matrix_alias_vi="Thuận pha mua",
                spot_futures_relation="CONFLUENT",
                venue_confirmation_state="CONFIRMED",
                liquidation_bias="SHORTS_FLUSHED",
                tradability_grade="A",
            )
        )
        await store.save_thesis(signal, opened_ts=1_000, entry_px=100.0)
        await store.init_outcomes(signal.thesis_id, ["24h"], opened_ts=1_000)
        await store.save_outcome(signal.thesis_id, "24h", realized_px=104.0, realized_high=105.0, realized_low=99.0)
        await store.finalize_thesis_from_outcome(signal.thesis_id, "24h", updated_at=90_000)
        return await store.get_matrix_scorecard()

    scorecard = asyncio.run(_run())

    assert scorecard[0]["matrix_cell"] == "POS_INIT__POS_INV"
    assert scorecard[0]["venue_confirmation_state"] == "CONFIRMED"
    assert scorecard[0]["liquidation_bias"] == "SHORTS_FLUSHED"
    assert scorecard[0]["horizons"]["24h"]["avg_edge"] > 0

    text = render_matrix_scorecard_vi(scorecard)
    assert "Bảng điểm matrix" in text
    assert "Thuận pha mua" in text


def test_measurement_renderers_include_matrix_highlights():
    matrix_scorecard = [
        {
            "matrix_cell": "POS_INIT__POS_INV",
            "matrix_alias_vi": "Thuận pha mua",
            "spot_futures_relation": "CONFLUENT",
            "venue_confirmation_state": "CONFIRMED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "tradability_grade": "A",
            "total_signals": 4,
            "horizons": {"24h": {"count": 4, "wins": 3, "win_rate": 0.75, "avg_edge": 2.4, "avg_mae": 12.0, "avg_mfe": 38.0}},
        },
        {
            "matrix_cell": "NEG_INIT__POS_INV",
            "matrix_alias_vi": "Bán gặp hấp thụ mua",
            "spot_futures_relation": "DIVERGENT",
            "venue_confirmation_state": "ALT_LEAD",
            "liquidation_bias": "LONGS_FLUSHED",
            "tradability_grade": "C",
            "total_signals": 3,
            "horizons": {"24h": {"count": 3, "wins": 1, "win_rate": 0.33, "avg_edge": -1.4, "avg_mae": 26.0, "avg_mfe": 11.0}},
        },
    ]
    stats = {
        "label": "2026-03-20",
        "opened_count": 7,
        "avg_score": 81.2,
        "avg_confidence": 0.78,
        "outcomes_count": 4,
        "positive_outcomes": 3,
        "avg_edge": 1.3,
        "avg_mae": 15.0,
        "avg_mfe": 31.0,
        "fill_count": 4,
        "setup_dist": {"breakout_ignition": 4},
        "stage_dist": {"ACTIONABLE": 4},
        "closed_stage_dist": {"RESOLVED": 3, "INVALIDATED": 1},
    }

    transition_scorecard = [
        {
            "transition_code": "CONTINUATION_TO_LONG",
            "transition_family": "CONTINUATION",
            "transition_alias_vi": "Tiếp diễn mua",
            "total_signals": 4,
            "avg_transition_speed": 0.71,
            "avg_transition_quality": 0.76,
            "horizons": {"24h": {"count": 4, "wins": 3, "win_rate": 0.75, "avg_edge": 1.9, "avg_mae": 12.0, "avg_mfe": 34.0}},
        },
        {
            "transition_code": "TRAP_TO_SHORT",
            "transition_family": "TRAP",
            "transition_alias_vi": "Pha bán có dấu hiệu bẫy",
            "total_signals": 3,
            "avg_transition_speed": 0.64,
            "avg_transition_quality": 0.28,
            "horizons": {"24h": {"count": 3, "wins": 1, "win_rate": 0.33, "avg_edge": -1.1, "avg_mae": 20.0, "avg_mfe": 8.0}},
        },
    ]

    flow_state_scorecard = [
        {
            "flow_state_code": "LONG_CONTINUATION__FOLLOW_THROUGH",
            "forced_flow_state": "SQUEEZE_LED",
            "inventory_defense_state": "BID_DEFENSE",
            "decision_posture": "AGGRESSIVE",
            "tradability_grade": "A",
            "avg_trap_risk": 0.18,
            "avg_forced_flow_intensity": 1.12,
            "avg_context_quality_score": 0.78,
            "total_signals": 4,
            "horizons": {"24h": {"count": 4, "wins": 3, "win_rate": 0.75, "avg_edge": 2.1, "avg_mae": 13.0, "avg_mfe": 35.0}},
        },
        {
            "flow_state_code": "SHORT_TRAP__FAILING_FOLLOW_THROUGH",
            "forced_flow_state": "NONE",
            "inventory_defense_state": "NONE",
            "decision_posture": "WAIT",
            "tradability_grade": "D",
            "avg_trap_risk": 0.71,
            "avg_forced_flow_intensity": 0.0,
            "avg_context_quality_score": 0.29,
            "total_signals": 3,
            "horizons": {"24h": {"count": 3, "wins": 1, "win_rate": 0.33, "avg_edge": -1.3, "avg_mae": 24.0, "avg_mfe": 9.0}},
        },
    ]

    forced_flow_scorecard = [
        {
            "forced_flow_state": "SQUEEZE_LED",
            "liquidation_bias": "SHORTS_FLUSHED",
            "basis_state": "BALANCED",
            "tradability_grade": "A",
            "avg_forced_flow_intensity": 1.2,
            "avg_liquidation_intensity": 1.1,
            "avg_trap_risk": 0.22,
            "total_signals": 4,
            "horizons": {"24h": {"count": 4, "wins": 3, "win_rate": 0.75, "avg_edge": 2.2, "avg_mae": 11.0, "avg_mfe": 37.0}},
        },
        {
            "forced_flow_state": "GAP_LED",
            "liquidation_bias": "MIXED",
            "basis_state": "WIDE_POSITIVE",
            "tradability_grade": "D",
            "avg_forced_flow_intensity": 0.84,
            "avg_liquidation_intensity": 0.46,
            "avg_trap_risk": 0.66,
            "total_signals": 3,
            "horizons": {"24h": {"count": 3, "wins": 1, "win_rate": 0.33, "avg_edge": -1.5, "avg_mae": 21.0, "avg_mfe": 10.0}},
        },
    ]

    daily_text = render_daily_summary_vi(
        stats,
        {"decision_counts": {}, "usefulness_counts": {}},
        matrix_scorecard,
        flow_state_scorecard,
        forced_flow_scorecard,
    )
    weekly_text = render_weekly_review_vi(
        stats,
        [],
        {},
        [],
        matrix_scorecard,
        transition_scorecard,
        flow_state_scorecard,
        forced_flow_scorecard,
    )
    flow_text = render_flow_state_scorecard_vi(flow_state_scorecard)
    forced_text = render_forced_flow_scorecard_vi(forced_flow_scorecard)
    transition_text = render_transition_scorecard_vi(transition_scorecard)

    assert "Matrix nổi bật" in daily_text
    assert "Flow state nổi bật" in daily_text
    assert "Forced flow đáng chú ý" in daily_text
    assert "Matrix tốt nhất" in weekly_text
    assert "Matrix yếu nhất" in weekly_text
    assert "Flow state tốt nhất" in weekly_text
    assert "Flow state yếu nhất" in weekly_text
    assert "Transition tốt nhất" in weekly_text
    assert "Transition yếu nhất" in weekly_text
    assert "Forced flow tốt nhất" in weekly_text
    assert "Forced flow rủi ro nhất" in weekly_text
    assert "Bảng điểm flow state" in flow_text
    assert "LONG_CONTINUATION__FOLLOW_THROUGH" in flow_text
    assert "Bảng điểm forced flow" in forced_text
    assert "SQUEEZE_LED" in forced_text
    assert "Bảng điểm transition" in transition_text
    assert "Tiếp diễn mua" in transition_text


def test_store_builds_transition_scorecard_with_phase3_metrics(tmp_path):
    db_path = tmp_path / "state.db"
    _bootstrap_db(db_path)
    store = ThesisSQLiteStore(db_path)

    signal = ThesisSignal(
        thesis_id="thesis-transition-1",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=84.0,
        confidence=0.78,
        coverage=0.82,
        why_now=["test"],
        conflicts=[],
        invalidation="below bid",
        entry_style="pullback",
        targets=["1h", "24h"],
    )

    async def _run():
        await store.migrate_schema()
        await store.save_thesis(signal, opened_ts=1_000, entry_px=100.0)
        await store.init_outcomes(signal.thesis_id, ["24h"], opened_ts=1_000)
        await store.save_outcome(signal.thesis_id, "24h", realized_px=104.0, realized_high=105.0, realized_low=99.0)
        await store.save_flow_transition(
            type(
                "Evt",
                (),
                {
                    "transition_id": "transition-1",
                    "symbol": "BTCUSDT",
                    "venue": "binance",
                    "timestamp": 1_050,
                    "from_cell": "POS_INIT__NEG_INV",
                    "to_cell": "POS_INIT__POS_INV",
                    "transition_code": "INVENTORY_CONFIRM_TO_LONG",
                    "transition_family": "INVENTORY_CONFIRM",
                    "transition_alias_vi": "Inventory xác nhận phe mua",
                    "from_flow_state_code": "LONG_TRAP_RISK",
                    "to_flow_state_code": "LONG_CONTINUATION__FOLLOW_THROUGH",
                    "transition_speed": 0.66,
                    "transition_quality": 0.74,
                    "persistence_score": 0.62,
                    "forced_flow_involved": False,
                    "trap_risk": 0.18,
                    "from_decision_posture": "CONSERVATIVE",
                    "to_decision_posture": "AGGRESSIVE",
                    "decision_shift": "CONSERVATIVE_TO_AGGRESSIVE",
                    "metadata": {"phase": 3},
                },
            )()
        )
        await store.finalize_thesis_from_outcome(signal.thesis_id, "24h", updated_at=90_000)
        return await store.get_transition_scorecard()

    scorecard = asyncio.run(_run())

    assert scorecard[0]["transition_family"] == "INVENTORY_CONFIRM"
    assert scorecard[0]["transition_alias_vi"] == "Inventory xác nhận phe mua"
    assert scorecard[0]["avg_transition_quality"] > 0.7
    assert scorecard[0]["avg_transition_speed"] > 0.6


def test_store_builds_flow_and_forced_flow_scorecards(tmp_path):
    db_path = tmp_path / "state.db"
    _bootstrap_db(db_path)
    store = ThesisSQLiteStore(db_path)

    signal = ThesisSignal(
        thesis_id="thesis-flow-1",
        instrument_key="BINANCE:BTCUSDT:SPOT",
        setup="breakout_ignition",
        direction="LONG_BIAS",
        stage="ACTIONABLE",
        score=87.0,
        confidence=0.82,
        coverage=0.84,
        why_now=["flow"],
        conflicts=[],
        invalidation="below bid",
        entry_style="retest",
        targets=["1h", "24h"],
    )

    async def _run():
        await store.migrate_schema()
        await store.save_tpfm_snapshot(
            TPFMSnapshot(
                snapshot_id="snap-flow-1",
                symbol="BTCUSDT",
                window_start_ts=900,
                window_end_ts=1_100,
                matrix_cell="POS_INIT__POS_INV",
                matrix_alias_vi="Thuận pha mua",
                flow_state_code="LONG_CONTINUATION__FOLLOW_THROUGH",
                forced_flow_state="SQUEEZE_LED",
                forced_flow_intensity=1.18,
                inventory_defense_state="BID_DEFENSE",
                decision_posture="AGGRESSIVE",
                tradability_grade="A",
                liquidation_bias="SHORTS_FLUSHED",
                basis_state="BALANCED",
                liquidation_intensity=1.06,
                trap_risk=0.21,
                context_quality_score=0.77,
            )
        )
        await store.save_thesis(signal, opened_ts=1_000, entry_px=100.0)
        await store.init_outcomes(signal.thesis_id, ["24h"], opened_ts=1_000)
        await store.save_outcome(signal.thesis_id, "24h", realized_px=104.0, realized_high=105.0, realized_low=99.0)
        await store.finalize_thesis_from_outcome(signal.thesis_id, "24h", updated_at=90_000)
        return await store.get_flow_state_scorecard(), await store.get_forced_flow_scorecard()

    flow_scorecard, forced_scorecard = asyncio.run(_run())

    assert flow_scorecard[0]["flow_state_code"] == "LONG_CONTINUATION__FOLLOW_THROUGH"
    assert flow_scorecard[0]["decision_posture"] == "AGGRESSIVE"
    assert flow_scorecard[0]["avg_forced_flow_intensity"] > 1.0
    assert flow_scorecard[0]["horizons"]["24h"]["avg_edge"] > 0
    assert forced_scorecard[0]["forced_flow_state"] == "SQUEEZE_LED"
    assert forced_scorecard[0]["liquidation_bias"] == "SHORTS_FLUSHED"
    assert forced_scorecard[0]["avg_liquidation_intensity"] > 1.0


def test_store_persists_vnext_flow_contract_fields(tmp_path):
    db_path = tmp_path / "state.db"
    _bootstrap_db(db_path)
    store = ThesisSQLiteStore(db_path)

    async def _run():
        await store.migrate_schema()
        await store.save_tpfm_snapshot(
            TPFMSnapshot(
                snapshot_id="snap-contract-1",
                symbol="BTCUSDT",
                window_start_ts=0,
                window_end_ts=300000,
                matrix_cell="POS_INIT__POS_INV",
                matrix_alias_vi="Thuận pha mua",
                flow_state_code="LONG_CONTINUATION__FOLLOW_THROUGH",
                forced_flow_state="SQUEEZE_LED",
                forced_flow_intensity=1.4,
                inventory_defense_state="BID_DEFENSE",
                decision_posture="AGGRESSIVE",
                decision_summary_vi="Ưu tiên continuation long khi bid còn đỡ",
                entry_condition_vi="Retest microprice giữ vững",
                confirm_needed_vi="Futures delta tiếp tục dương",
                avoid_if_vi="Basis nở quá nhanh",
                review_tags_json='{"matrix_cell":"POS_INIT__POS_INV"}',
            )
        )

    asyncio.run(_run())

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        """
        SELECT flow_state_code, forced_flow_intensity, inventory_defense_state,
               decision_summary_vi, entry_condition_vi, confirm_needed_vi, avoid_if_vi, review_tags_json
        FROM tpfm_m5_snapshot
        WHERE snapshot_id = 'snap-contract-1'
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "LONG_CONTINUATION__FOLLOW_THROUGH"
    assert row[1] == 1.4
    assert row[2] == "BID_DEFENSE"
    assert "continuation long" in row[3]
    assert row[4] == "Retest microprice giữ vững"
    assert row[5] == "Futures delta tiếp tục dương"
    assert row[6] == "Basis nở quá nhanh"
    assert "matrix_cell" in row[7]
