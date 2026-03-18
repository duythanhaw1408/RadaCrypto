from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ExecutionSide = Literal["BUY", "SELL"]
OrderStatus = Literal["NEW", "ACKED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"]
OrderType = Literal["LIMIT", "MARKET"]
Liquidity = Literal["MAKER", "TAKER", "UNKNOWN"]


@dataclass(slots=True)
class CanonicalOrder:
    order_id: str
    client_order_id: str
    venue: str
    account_id: str
    symbol: str
    side: ExecutionSide
    order_type: OrderType
    time_in_force: str
    qty: float
    price: float | None
    created_ts: int


@dataclass(slots=True)
class OrderSnapshot:
    order: CanonicalOrder
    status: OrderStatus
    filled_qty: float = 0.0
    quote_filled: float = 0.0
    avg_fill_price: float | None = None
    fee_paid: float = 0.0
    updated_ts: int = 0


@dataclass(slots=True)
class FillFact:
    fill_id: str
    order_id: str
    venue: str
    account_id: str
    symbol: str
    side: ExecutionSide
    qty: float
    price: float
    fee_paid: float
    fee_asset: str
    liquidity: Liquidity
    venue_ts: int


@dataclass(slots=True)
class PositionReconciliation:
    symbol: str
    internal_net_qty: float
    venue_net_qty: float
    delta_qty: float
    gross_buy_qty: float = 0.0
    gross_sell_qty: float = 0.0
    unique_fill_count: int = 0
    duplicate_fill_count: int = 0
    out_of_order_fill_count: int = 0
    qty_violation_count: int = 0
    has_structural_issues: bool = False
    is_aligned: bool = False


@dataclass(slots=True)
class ExecutionQualityMetrics:
    fill_count: int
    avg_slippage_bps: float
    avg_markout_bps: float


@dataclass(slots=True)
class ExecutionSummary:
    total_orders: int
    open_orders: int
    closed_orders: int
    total_fills: int
    reconciliation: PositionReconciliation
    quality: ExecutionQualityMetrics
