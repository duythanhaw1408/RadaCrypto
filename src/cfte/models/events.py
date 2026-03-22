from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

TakerSide = Literal["BUY", "SELL"]
Stage = Literal["DETECTED", "WATCHLIST", "CONFIRMED", "ACTIONABLE", "INVALIDATED", "RESOLVED"]
Direction = Literal["LONG_BIAS", "SHORT_BIAS"]
Setup = Literal[
    "stealth_accumulation",
    "breakout_ignition",
    "distribution",
    "failed_breakout",
]


LiquidationBias = Literal["NONE", "LONGS_FLUSHED", "SHORTS_FLUSHED", "MIXED"]


@dataclass(slots=True)
class NormalizedTrade:
    event_id: str
    venue: str
    instrument_key: str
    price: float
    qty: float
    quote_qty: float
    taker_side: TakerSide
    venue_ts: int


@dataclass(slots=True)
class NormalizedBookTop:
    event_id: str
    venue: str
    instrument_key: str
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float
    venue_ts: int


@dataclass(slots=True)
class NormalizedDepthDiff:
    event_id: str
    venue: str
    instrument_key: str
    first_update_id: int
    final_update_id: int
    bid_updates: list[tuple[float, float]]
    ask_updates: list[tuple[float, float]]
    venue_ts: int


@dataclass(slots=True)
class NormalizedKline:
    event_id: str
    venue: str
    instrument_key: str
    interval: str
    open_px: float
    high_px: float
    low_px: float
    close_px: float
    base_volume: float
    quote_volume: float
    open_ts: int
    close_ts: int
    is_closed: bool
    venue_ts: int


@dataclass(slots=True)
class TapeSnapshot:
    instrument_key: str
    window_start_ts: int
    window_end_ts: int
    spread_bps: float
    microprice: float
    imbalance_l1: float
    delta_quote: float
    cvd: float
    trade_burst: float
    absorption_proxy: float
    bid_px: float
    ask_px: float
    mid_px: float
    last_trade_px: float
    trade_count: int
    futures_delta: float = 0.0
    liquidation_vol: float = 0.0
    liquidation_bias: LiquidationBias = "NONE"
    venue_confirmation_state: str = "UNCONFIRMED"
    leader_venue: str = "UNKNOWN"
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class ThesisSignal:
    thesis_id: str
    instrument_key: str
    setup: Setup
    direction: Direction
    stage: Stage
    score: float
    confidence: float
    coverage: float
    why_now: list[str]
    conflicts: list[str]
    invalidation: str
    entry_style: str
    targets: list[str]
    timeframe: str = "1h"
    regime_bucket: str = "NEUTRAL"
    flow_state: str = ""
    matrix_cell: str = ""
    matrix_alias_vi: str = ""
    tradability_grade: str = ""
    decision_posture: str = ""
    decision_summary_vi: str = ""
    flow_alignment_score: float = 0.0
    ai_brief_vi: str = ""
    edge_score: float = 0.0
    edge_confidence: str = "LOW"


@dataclass(slots=True)
class ThesisOutcome:
    thesis_id: str
    horizon: str
    status: str
    target_ts: int
    realized_px: float | None = None
    realized_high: float | None = None
    realized_low: float | None = None
    fill_px: float | None = None
    mae_bps: float | None = None
    mfe_bps: float | None = None
    exit_ts: int | None = None
