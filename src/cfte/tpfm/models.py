from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass(slots=True)
class TPFMSnapshot:
    """M5 Sufficient Statistics Snapshot for TPFM"""
    snapshot_id: str
    symbol: str = "BTCUSDT"
    venue: str = "binance"
    window_start_ts: int = 0
    window_end_ts: int = 0

    # Polarity & Scores
    initiative_score: float = 0.0
    initiative_polarity: str = "NEUTRAL_INIT"  # POS_INIT, NEG_INIT, NEUTRAL_INIT
    inventory_score: float = 0.0
    inventory_polarity: str = "NEUTRAL_INV"   # POS_INV, NEG_INV, NEUTRAL_INV

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
    microprice_gap_bps: float = 0.0
    spread_bps: float = 0.0

    # Context Overlay (Phase T5 - Real-world Refinement)
    context_score: float = 0.0
    basis_divergence: float = 0.0 # Legacy field
    futures_bias_proxy: str = "NEUTRAL_FUTURES" # Legacy field

    # New High-Fidelity Context Fields
    futures_context_available: bool = False
    futures_context_fresh: bool = False
    funding_rate: float = 0.0
    funding_bias: str = "NEUTRAL" # POSITIVE, NEGATIVE, NEUTRAL
    basis_bps: float = 0.0
    basis_divergence_state: str = "ALIGNED" # ALIGNED, DIVERGING_POS, DIVERGING_NEG
    oi_value: float = 0.0
    oi_delta: float = 0.0
    oi_state: str = "STABLE" # EXPANDING, CONTRACTING, STABLE
    futures_pressure_bias: str = "NEUTRAL" # BULLISH, BEARISH, NEUTRAL
    context_quality_score: float = 0.0
    context_warning_flags: List[str] = field(default_factory=list)

    # Health
    degraded: bool = False
    health_state: str = "HEALTHY"

    # Escalation
    should_escalate: bool = False
    escalation_reason: List[str] = field(default_factory=list)


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

    # AI Analysis
    ai_analysis_vi: str = ""
    should_send_ai_report: bool = False
