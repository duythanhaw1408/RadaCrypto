from __future__ import annotations

from dataclasses import dataclass

from cfte.models.events import NormalizedTrade
from cfte.normalizers.instruments import parse_instrument_key


@dataclass(slots=True)
class VenueFlow:
    venue: str
    trade_count: int
    total_quote: float
    vwap: float


@dataclass(slots=True)
class VenueComparisonResult:
    symbol: str
    market_type: str
    flows: list[VenueFlow]
    leader_venue: str
    lagger_venue: str
    vwap_spread_bps: float


def compare_trade_flows(trades: list[NormalizedTrade]) -> VenueComparisonResult:
    if not trades:
        raise ValueError("trades must not be empty")

    _, symbol, market_type = parse_instrument_key(trades[0].instrument_key)
    buckets: dict[str, list[NormalizedTrade]] = {}
    for trade in trades:
        _, trade_symbol, trade_market_type = parse_instrument_key(trade.instrument_key)
        if trade_symbol != symbol or trade_market_type != market_type:
            continue
        buckets.setdefault(trade.venue, []).append(trade)

    flows: list[VenueFlow] = []
    for venue, venue_trades in buckets.items():
        total_quote = sum(t.quote_qty for t in venue_trades)
        if total_quote <= 0:
            vwap = 0.0
        else:
            vwap = sum(t.price * t.quote_qty for t in venue_trades) / total_quote
        flows.append(VenueFlow(venue=venue, trade_count=len(venue_trades), total_quote=total_quote, vwap=vwap))

    if len(flows) < 2:
        raise ValueError("at least two venues are required for comparison")

    flows.sort(key=lambda flow: flow.total_quote, reverse=True)
    leader = flows[0]
    lagger = flows[-1]

    max_vwap = max(flow.vwap for flow in flows)
    min_vwap = min(flow.vwap for flow in flows)
    vwap_spread_bps = ((max_vwap - min_vwap) / max_vwap) * 10_000 if max_vwap > 0 else 0.0

    return VenueComparisonResult(
        symbol=symbol,
        market_type=market_type,
        flows=flows,
        leader_venue=leader.venue,
        lagger_venue=lagger.venue,
        vwap_spread_bps=vwap_spread_bps,
    )


def render_venue_comparison_vi(result: VenueComparisonResult) -> str:
    lines = [
        f"So sánh đa sàn cho {result.symbol} ({result.market_type})",
        f"Sàn dẫn nhịp theo quote volume: {result.leader_venue}",
        f"Sàn theo sau: {result.lagger_venue}",
        f"Độ lệch VWAP liên sàn: {result.vwap_spread_bps:.2f} bps",
        "Chi tiết từng sàn:",
    ]
    for flow in result.flows:
        lines.append(
            f"- {flow.venue}: {flow.trade_count} lệnh, quote={flow.total_quote:.2f}, VWAP={flow.vwap:.2f}"
        )
    return "\n".join(lines)
