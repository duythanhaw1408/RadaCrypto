from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

@dataclass(slots=True)
class TPFMSnapshot:
    """M5 Sufficient Statistics Snapshot for TPFM"""
    snapshot_id: str
    symbol: str = "BTCUSDT"
    venue: str = "binance"
    window_start_ts: int = 0
    window_end_ts: int = 0
    run_id: str = ""
    microprice: float = 0.0
    open_px: float = 0.0
    high_px: float = 0.0
    low_px: float = 0.0
    close_px: float = 0.0
    volume_quote: float = 0.0

    # Polarity & Scores
    initiative_score: float = 0.0
    initiative_polarity: str = "NEUTRAL_INIT"  # POS_INIT, NEG_INIT, NEUTRAL_INIT
    initiative_strength: float = 0.0
    inventory_score: float = 0.0
    inventory_polarity: str = "NEUTRAL_INV"   # POS_INV, NEG_INV, NEUTRAL_INV
    inventory_strength: float = 0.0
    axis_confidence: float = 0.0

    # Energy & Efficiency
    energy_score: float = 0.0
    energy_state: str = "COMPRESSION"           # COMPRESSION, EXPANDING, EXHAUSTING
    response_efficiency_score: float = 0.0
    response_efficiency_state: str = "MIXED"    # FOLLOW_THROUGH, MIXED, ABSORBED_OR_TRAP

    # Confidence & Quality
    conflict_score: float = 0.0
    agreement_score: float = 0.0
    tradability_score: float = 0.0
    market_quality_score: float = 0.0
    novelty_score: float = 0.0

    # Matrix
    matrix_cell: str = "NEUTRAL_INIT__NEUTRAL_INV"
    micro_conclusion: str = "UNCERTAIN"
    matrix_alias_vi: str = "Trung tính"
    continuation_bias: str = "NEUTRAL"
    preferred_posture: str = "Đứng ngoài và chờ cấu trúc rõ hơn"
    tradability_grade: str = "D"

    # Setup Context
    dominant_setups: List[str] = field(default_factory=list)
    setup_score_map: Dict[str, float] = field(default_factory=dict)

    # Signal Lifecycle Stats
    active_thesis_count: int = 0
    new_thesis_count: int = 0
    actionable_count: int = 0
    invalidated_count: int = 0
    resolved_count: int = 0

    # Raw Feature Proxies
    delta_quote: float = 0.0
    cvd_slope: float = 0.0
    trade_burst: float = 0.0
    absorption_score: float = 0.0
    imbalance_l1: float = 0.0
    centered_imbalance_l1: float = 0.0
    signed_absorption_score: float = 0.0
    microprice_gap_bps: float = 0.0
    spread_bps: float = 0.0
    
    # Phase 2 Metrics
    delta_zscore: float = 0.0
    aggression_ratio: float = 0.5 # 0.5 is neutral
    sweep_quote: float = 0.0
    sweep_buy_quote: float = 0.0
    sweep_sell_quote: float = 0.0
    burst_persistence: float = 0.0
    microprice_drift_bps: float = 0.0
    replenishment_bid_score: float = 0.0
    replenishment_ask_score: float = 0.0

    # Context Overlay (Phase T5 - Real-world Refinement)
    context_score: float = 0.0
    basis_divergence: float = 0.0 # Legacy field
    futures_bias_proxy: str = "NEUTRAL_FUTURES" # Legacy field

    # New High-Fidelity Context Fields
    futures_context_available: bool = False
    futures_context_fresh: bool = False
    futures_delta_available: bool = False
    futures_delta: float = 0.0
    funding_rate: float = 0.0
    funding_bias: str = "NEUTRAL" # POSITIVE, NEGATIVE, NEUTRAL
    basis_bps: float = 0.0
    basis_divergence_state: str = "ALIGNED" # ALIGNED, DIVERGING_POS, DIVERGING_NEG
    oi_value: float = 0.0
    oi_delta: float = 0.0
    oi_expansion_ratio: float = 0.0
    oi_state: str = "STABLE" # EXPANDING, CONTRACTING, STABLE
    futures_pressure_bias: str = "NEUTRAL" # BULLISH, BEARISH, NEUTRAL
    futures_aggression_ratio: float = 0.5
    spot_futures_relation: str = "NO_FUTURES_CONFIRM"
    context_quality_score: float = 0.0
    context_warning_flags: List[str] = field(default_factory=list)
    basis_state: str = "BALANCED"
    venue_confirmation_state: str = "UNCONFIRMED"
    leader_venue: str = ""
    lagger_venue: str = ""
    leader_confidence: float = 0.0
    aligned_window_ms: int = 0
    venue_vwap_spread_bps: float = 0.0
    liquidation_context_available: bool = False
    liquidation_bias: str = "UNKNOWN"
    liquidation_count: int = 0
    liquidation_quote: float = 0.0

    # vNext High-Fidelity Flow Fields
    flow_state_code: str = "NEUTRAL"
    forced_flow_state: str = "NONE" # NONE, LIQUIDATION_LED, SQUEEZE_LED, GAP_LED
    forced_flow_intensity: float = 0.0
    liquidation_intensity: float = 0.0
    inventory_defense_state: str = "NONE" # NONE, BID_DEFENSE, ASK_DEFENSE
    transition_ready: bool = False
    trap_risk: float = 0.0
    decision_posture: str = "WAIT" # LONG, SHORT, WAIT, EXIT
    decision_summary_vi: str = "Đứng ngoài cho tới khi dòng tiền rõ hơn."
    entry_condition_vi: str = "N/A"
    confirm_needed_vi: str = "N/A"
    avoid_if_vi: str = "N/A"
    review_tags: Dict[str, str] = field(default_factory=dict)
    review_tags_json: str = "{}"  # Legacy persistence bridge

    # Compatibility aliases for pre-vNext callers
    entry_condition: str = "N/A"
    avoid_if: str = "N/A"

    # Phase A: Temporal Deltas (M5)
    initiative_delta_1: float = 0.0
    initiative_delta_3: float = 0.0
    initiative_delta_5: float = 0.0
    inventory_delta_1: float = 0.0
    inventory_delta_3: float = 0.0
    inventory_delta_5: float = 0.0
    agreement_delta_3: float = 0.0
    tradability_delta_3: float = 0.0
    forced_flow_delta_3: float = 0.0
    tempo_state: str = "UNKNOWN" # ACCELERATING, DECELERATING, STABLE
    persistence_state: str = "UNKNOWN" # NEW, PERSISTENT, EXHAUSTING
    exhaustion_risk: float = 0.0
    history_depth: int = 0

    # Phase B: Sequence Tracking
    sequence_id: str = ""
    sequence_signature: str = "UNKNOWN"
    sequence_length: int = 0
    sequence_family: str = "UNKNOWN"
    sequence_quality: float = 0.0
    
    # Phase 1: Pattern Base
    pattern_code: str = "UNCLASSIFIED"
    pattern_alias_vi: str = "Chưa phân loại pattern"
    pattern_family: str = "NONE"
    pattern_phase: str = "FORMING"
    pattern_strength: float = 0.0
    pattern_quality: float = 0.0
    pattern_failure_risk: float = 0.0
    
    # Phase 2: Sequence Advanced & MTF
    sequence_start_ts: float = 0.0
    sequence_duration_sec: float = 0.0
    is_sequence_pivot: bool = False
    
    # Phase 3: Probability & Expectancy
    edge_score: float = 0.0
    edge_confidence: str = "LOW"
    historical_win_rate: float = 0.0
    expected_rr: float = 0.0
    
    parent_context: Dict[str, object] = field(default_factory=dict)
    
    # Outcome tracking (T-plus)
    t_plus_1_price: float = 0.0
    t_plus_5_price: float = 0.0
    t_plus_12_price: float = 0.0
    
    # Health
    degraded: bool = False
    health_state: str = "HEALTHY"
    blind_spot_flags: List[str] = field(default_factory=list)

    # Escalation & Facts
    should_escalate: bool = False
    escalation_reason: List[str] = field(default_factory=list)
    observed_facts: List[str] = field(default_factory=list)
    inferred_facts: List[str] = field(default_factory=list)
    missing_context: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    action_plan_vi: str = "Đứng ngoài"
    flow_decision_brief: str = "" # Final AI-generated trader brief
    invalid_if: str = "Matrix không còn giữ được hướng hiện tại"
    metadata: Dict[str, object] = field(default_factory=dict)
    transition_event: FlowTransitionEvent | None = None
    stack_state: 'FlowStackState' | None = None
    edge_profile: 'ProbabilityEdge' | None = None

    def __iter__(self):
        yield self
        yield self.transition_event
        yield self.stack_state
        yield self.edge_profile


@dataclass(slots=True)
class FlowTransitionEvent:
    """Represents a change in the Matrix Cell / Flow State"""
    transition_id: str
    symbol: str
    venue: str
    timestamp: int
    from_cell: str
    to_cell: str
    transition_code: str # e.g. NEUTRAL_TO_CONTINUATION, FLIP_TO_SHORT
    transition_speed: float
    transition_quality: float
    persistence_score: float
    transition_family: str = "STRUCTURE_SHIFT"
    transition_alias_vi: str = "Chuyển pha cấu trúc"
    from_flow_state_code: str = "NEUTRAL_BALANCE"
    to_flow_state_code: str = "NEUTRAL_BALANCE"
    forced_flow_involved: bool = False
    trap_risk: float = 0.0
    from_decision_posture: str = "WAIT"
    to_decision_posture: str = "WAIT"
    decision_shift: str = "NONE"
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class FlowDecisionView:
    """The final contract for tradeable flow analysis"""
    decision_id: str
    snapshot_id: str
    symbol: str
    venue: str
    timestamp: int
    
    flow_bias: str # LONG, SHORT, NEUTRAL
    continuation_bias: str # CONTINUATION, REVERSAL, TRAP
    posture: str # AGGRESSIVE, CONSERVATIVE, WAIT
    
    # Output Contract
    tradability_grade: str
    entry_condition: str
    confirm_needed: str
    avoid_if: str
    invalid_if: str
    tp_path: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    review_tags: Dict[str, str] = field(default_factory=dict)
    
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class TPFM30mRegime:
    """30-minute Regime Synthesis for TPFM"""
    regime_id: str
    symbol: str = "BTCUSDT"
    venue: str = "binance"
    window_start_ts: int = 0
    window_end_ts: int = 0

    # Aggregation
    m5_count: int = 0
    dominant_cell: str = "NEUTRAL"
    dominant_regime: str = "NEUTRAL"
    transition_path: List[str] = field(default_factory=list)

    # Persistence & Consistency
    regime_persistence_score: float = 0.0
    conflict_score: float = 0.0
    agreement_score: float = 0.0
    tradability_score: float = 0.0
    market_quality_score: float = 0.0

    # Density & Novelty
    actionability_density: float = 0.0
    invalidation_pressure_score: float = 0.0
    novelty_score: float = 0.0

    # Setup Stats
    dominant_setup_frequency: Dict[str, int] = field(default_factory=dict)

    # Flow Aggregates
    net_delta_quote: float = 0.0
    net_cvd_slope: float = 0.0
    avg_trade_burst: float = 0.0
    peak_trade_burst: float = 0.0

    # Conclusions
    macro_conclusion_code: str = "NEUTRAL"
    macro_posture: str = "WAIT_FOR_REGIME"

    # Status
    degraded: bool = False
    health_state: str = "HEALTHY"

    # AI Flags
    should_send_ai_summary: bool = False
    ai_summary_reason: List[str] = field(default_factory=list)


@dataclass(slots=True)
class TPFM4hStructural:
    """4-hour Structural Flow matrix for TPFM"""
    structural_id: str
    symbol: str = "BTCUSDT"
    venue: str = "binance"
    window_start_ts: int = 0
    window_end_ts: int = 0

    m30_count: int = 0

    # Share analysis
    dominant_regime_share: Dict[str, float] = field(default_factory=dict)
    dominant_cell_share: Dict[str, float] = field(default_factory=dict)

    # Bias & Quality
    structural_bias: str = "NEUTRAL"
    structural_quality: str = "LOW"
    structural_actionability: str = "LOW"

    # Transitions
    transition_map: List[str] = field(default_factory=list)

    # Aggregates
    net_delta_quote: float = 0.0
    avg_persistence: float = 0.0
    structural_score: float = 0.0

    # Health
    health_state: str = "HEALTHY"

    # AI Analysis
    ai_analysis_vi: str = ""
    should_send_ai_report: bool = False


@dataclass(slots=True)
class FlowSequenceEvent:
    """Phase B: Sequence tracking event for continuous flow patterns"""
    sequence_id: str
    symbol: str
    venue: str
    started_ts: int
    ended_ts: int
    sequence_signature: str
    sequence_family: str
    sequence_length: int
    sequence_bias: str
    sequence_strength: float
    sequence_maturity: str
    sequence_quality: float
    stack_alignment_hint: str
    current_cell: str
    current_flow_state: str
    resolution_hint: str
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class FlowFrameState:
    """Phase C: Individual timeframe flow assessment"""
    frame: str # M5, M30, H1, H4
    dominant_cell: str
    dominant_alias_vi: str
    flow_bias: str
    tempo_state: str
    sequence_family: str
    tradability_grade: str
    agreement_score: float


@dataclass(slots=True)
class FlowStackState:
    """Phase C: Multi-timeframe synthesis"""
    stack_id: str
    symbol: str
    venue: str
    timestamp: int
    stack_signature: str
    stack_alignment: str
    stack_conflict: str
    micro_vs_macro: str
    stack_pressure: float
    stack_quality: float
    frames: Dict[str, FlowFrameState] = field(default_factory=dict)
    metadata: Dict[str, object] = field(default_factory=dict)

@dataclass(slots=True)
class FlowPatternEvent:
    """Phase 1: Matrix-Native Pattern Event"""
    pattern_id: str
    snapshot_id: str
    symbol: str
    venue: str
    timestamp: int
    pattern_code: str
    pattern_alias_vi: str
    pattern_family: str
    pattern_phase: str
    sequence_id: str
    sequence_signature: str
    sequence_length: int
    tempo_state: str
    persistence_state: str
    pattern_strength: float
    pattern_quality: float
    pattern_failure_risk: float
    matrix_cell: str
    flow_state_code: str
    metadata: Dict[str, object] = field(default_factory=dict)

@dataclass(slots=True)
class PatternOutcome:
    """Phase 2: Performance tracking for specific patterns/sequences"""
    outcome_id: str
    snapshot_id: str
    symbol: str
    timestamp: int
    pattern_code: str
    sequence_signature: str
    
    # Prices
    start_px: float
    t1_px: float = 0.0  # +5m
    t5_px: float = 0.0  # +25m
    t12_px: float = 0.0 # +60m
    
    # Returns (bps)
    r1_bps: float = 0.0
    r5_bps: float = 0.0
    r12_bps: float = 0.0
    
    max_favorable_bps: float = 0.0
    max_adverse_bps: float = 0.0
    
    metadata: Dict[str, object] = field(default_factory=dict)
