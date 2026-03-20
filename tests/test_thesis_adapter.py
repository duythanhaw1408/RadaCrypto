
import pytest
from unittest.mock import MagicMock
from cfte.models.events import TapeSnapshot, ThesisSignal
from cfte.tpfm.models import TPFMSnapshot
from cfte.thesis.engines import evaluate_setups

def test_evaluate_setups_respects_tpfm_alignment():
    # Setup dummy snapshot (borderline, about score 60-70)
    snapshot = TapeSnapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        window_start_ts=1000,
        window_end_ts=2000,
        spread_bps=10.0,
        microprice=60000.0,
        imbalance_l1=0.52,
        delta_quote=1500.0,
        cvd=1500.0,
        trade_burst=1.8,
        absorption_proxy=30.0,
        bid_px=59999.0,
        ask_px=60001.0,
        mid_px=60000.0,
        last_trade_px=60000.5,
        trade_count=10
    )
    
    # 1. Evaluate WITHOUT TPFM (Base)
    signals_base = evaluate_setups(snapshot, tpfm_snapshot=None)
    bo_base = [s for s in signals_base if s.setup == "breakout_ignition"][0]
    
    # 2. Evaluate WITH ALIGNED TPFM (Long initiative)
    tpfm_aligned = TPFMSnapshot(
        snapshot_id="test_aligned",
        matrix_cell="POS_INIT__POS_INV",
        flow_state_code="LONG_CONTINUATION",
        matrix_alias_vi="Thuận pha mua",
        tradability_grade="A"
    )
    signals_aligned = evaluate_setups(snapshot, tpfm_snapshot=tpfm_aligned)
    bo_aligned = [s for s in signals_aligned if s.setup == "breakout_ignition"][0]
    
    # Score should be higher
    assert bo_aligned.score > bo_base.score
    assert any("Đồng thuận TPFM" in rw for rw in bo_aligned.why_now)
    assert bo_aligned.tradability_grade == "A"
    assert bo_aligned.matrix_alias_vi == "Thuận pha mua"
    assert bo_aligned.decision_posture == "AGGRESSIVE"
    assert signals_aligned[0].setup == "breakout_ignition"

    # 3. Evaluate WITH DIVERGENT TPFM (Short initiative)
    tpfm_divergent = TPFMSnapshot(
        snapshot_id="test_div",
        matrix_cell="NEG_INIT__NEG_INV",
        flow_state_code="SHORT_CONTINUATION",
        matrix_alias_vi="Thuận pha bán"
    )
    signals_div = evaluate_setups(snapshot, tpfm_snapshot=tpfm_divergent)
    bo_div = [s for s in signals_div if s.setup == "breakout_ignition"][0]
    
    # Score should be much lower (penalized)
    assert bo_div.score < bo_base.score
    assert any("Ngược pha TPFM" in c for c in bo_div.conflicts)

def test_evaluate_setups_governed_by_trap_risk():
    snapshot = TapeSnapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        window_start_ts=1000,
        window_end_ts=2000,
        spread_bps=5.0,
        microprice=60000.0,
        imbalance_l1=0.6,
        delta_quote=10000.0,
        cvd=10000.0,
        trade_burst=3.0,
        absorption_proxy=70.0,
        bid_px=59999.0,
        ask_px=60001.0,
        mid_px=60000.0,
        last_trade_px=60002.0,
        trade_count=100
    )
    
    tpfm_trap = TPFMSnapshot(
        snapshot_id="test_trap",
        matrix_cell="POS_INIT__NEG_INV", # Potential trap
        trap_risk=0.75,
        matrix_alias_vi="Mua đuổi rủi ro cao"
    )
    
    signals = evaluate_setups(snapshot, tpfm_snapshot=tpfm_trap)
    bo_signal = [s for s in signals if s.setup == "breakout_ignition"][0]
    
    # High trap risk should force score down
    assert bo_signal.score <= 60.0
    assert any("Rủi ro bẫy TPFM cao" in c for c in bo_signal.conflicts)


def test_evaluate_setups_uses_flow_entry_and_invalidation_as_primary_contract():
    snapshot = TapeSnapshot(
        instrument_key="BINANCE:BTCUSDT:SPOT",
        window_start_ts=1000,
        window_end_ts=2000,
        spread_bps=4.0,
        microprice=60000.0,
        imbalance_l1=0.63,
        delta_quote=12000.0,
        cvd=12000.0,
        trade_burst=3.2,
        absorption_proxy=75.0,
        bid_px=59999.0,
        ask_px=60001.0,
        mid_px=60000.0,
        last_trade_px=60002.0,
        trade_count=120,
    )

    tpfm = TPFMSnapshot(
        snapshot_id="test_contract",
        matrix_cell="POS_INIT__POS_INV",
        matrix_alias_vi="Thuận pha mua",
        flow_state_code="LONG_CONTINUATION__FOLLOW_THROUGH",
        tradability_grade="A",
        decision_posture="AGGRESSIVE",
        decision_summary_vi="Ưu tiên continuation long",
        entry_condition_vi="Microprice retest + Bid replenishment",
        invalid_if="Initiative mất dương hoặc inventory không còn đỡ.",
    )

    signals = evaluate_setups(snapshot, tpfm_snapshot=tpfm)
    breakout = [s for s in signals if s.setup == "breakout_ignition"][0]

    assert breakout.entry_style == "Microprice retest + Bid replenishment"
    assert breakout.invalidation == "Initiative mất dương hoặc inventory không còn đỡ."
    assert breakout.decision_summary_vi == "Ưu tiên continuation long"
