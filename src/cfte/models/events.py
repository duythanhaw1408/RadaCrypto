from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

TakerSide = Literal["BUY", "SELL"]
Stage = Literal["DETECTED", "WATCHLIST", "CONFIRMED", "ACTIONABLE", "INVALIDATED", "RESOLVED"]

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
    metadata: dict = field(default_factory=dict)

@dataclass(slots=True)
class ThesisSignal:
    thesis_id: str
    instrument_key: str
    setup: str
    direction: str
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
