import pytest
from datetime import datetime
from cfte.tpfm.engine import TPFMStateEngine
from cfte.tpfm.models import TPFMSnapshot

@pytest.fixture
def engine():
    return TPFMStateEngine()

def _make_dummy_snap(matrix_cell="NEUTRAL_INIT__NEUTRAL_INV", initiative=0.0, inventory=0.0):
    return TPFMSnapshot(
        snapshot_id="test_snap",
        symbol="BTCUSDT",
        venue="BINANCE",
        window_start_ts=1700000000000,
        window_end_ts=1700000300000,
        initiative_score=initiative,
        inventory_score=inventory,
        matrix_cell=matrix_cell,
        flow_state_code="TEST",
        matrix_alias_vi="Test",
        tradability_score=0.5,
        agreement_score=0.5
    )

def test_temporal_deltas_are_derived_from_recent_snapshots(engine):
    # Setup history
    s1 = _make_dummy_snap(initiative=0.1, inventory=0.1)
    s2 = _make_dummy_snap(initiative=0.2, inventory=0.2)
    engine._recent_snapshots = [s1, s2]
    
    # Current snap
    curr = _make_dummy_snap(initiative=0.5, inventory=0.3)
    engine._derive_temporal_memory(curr)
    
    assert curr.initiative_delta_1 == pytest.approx(0.3)
    assert curr.inventory_delta_1 == pytest.approx(0.1)
    assert curr.history_depth == 2

def test_pos_pos_repetition_maps_to_conti_long(engine):
    # Two consecutive POS_POS
    s1 = _make_dummy_snap(matrix_cell="POS_INIT__POS_INV", initiative=0.5)
    engine._derive_sequence_tracking(s1)
    engine._recent_snapshots.append(s1)
    
    s2 = _make_dummy_snap(matrix_cell="POS_INIT__POS_INV", initiative=0.6)
    engine._derive_sequence_tracking(s2)
    engine._derive_temporal_memory(s2)
    s2.tempo_state = engine._classify_tempo_state(s2)
    s2.persistence_state = engine._classify_persistence_state(s2)
    s2.exhaustion_risk = engine._estimate_exhaustion_risk(s2, engine._recent_snapshots)
    
    engine._derive_matrix_native_pattern(s2)
    engine._derive_pattern_phase(s2)
    
    assert s2.pattern_code == "CONTI_LONG"
    assert s2.sequence_length == 2
    assert s2.pattern_phase == "CONFIRMED"

def test_pos_neg_maps_to_trap_long_when_efficiency_is_bad(engine):
    s1 = _make_dummy_snap(matrix_cell="POS_INIT__NEG_INV")
    s1.response_efficiency_state = "ABSORBED_OR_TRAP"
    s1.trap_risk = 0.6
    
    engine._derive_matrix_native_pattern(s1)
    engine._derive_pattern_phase(s1)
    
    assert s1.pattern_code == "TRAP_LONG"
    # Phase will be FORMING because length is 1
    assert s1.pattern_phase == "FORMING"

def test_forced_flow_overrides_into_squeeze_long(engine):
    s1 = _make_dummy_snap(initiative=0.8)
    s1.forced_flow_state = "SQUEEZE_LED"
    s1.delta_quote = 1000.0
    
    engine._derive_matrix_native_pattern(s1)
    assert s1.pattern_code == "SQUEEZE_LONG"

def test_pattern_phase_goes_exhausting_when_persistent_but_decelerating(engine):
    # Set sequence length to 5
    s1 = _make_dummy_snap()
    s1.sequence_length = 5
    s1.persistence_state = "PERSISTENT"
    s1.tempo_state = "DECELERATING"
    s1.exhaustion_risk = 0.6
    s1.response_efficiency_state = "ABSORBED_OR_TRAP"
    
    engine._derive_pattern_phase(s1)
    assert s1.pattern_phase == "EXHAUSTING"

def test_sequence_signature_has_expected_format(engine):
    s1 = _make_dummy_snap(matrix_cell="POS_INIT__POS_INV")
    s1.sequence_length = 4
    s1.tempo_state = "ACCELERATING"
    s1.forced_flow_state = "NONE"
    s1.response_efficiency_state = "FOLLOW_THROUGH"
    
    sig = engine._build_sequence_signature(s1)
    assert sig == "POS_POSx4|ACCELERATING|NONE|FOLLOW_THROUGH"
