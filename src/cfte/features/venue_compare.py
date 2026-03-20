from __future__ import annotations

from dataclasses import dataclass

from cfte.models.events import NormalizedTrade
from cfte.normalizers.instruments import parse_instrument_key

MAX_VENUE_STALENESS_MS = 2_000
MAX_VENUE_WINDOW_SKEW_MS = 2_000

_EXCLUSION_REASON_LABELS_VI: dict[str, str] = {
    "stale_window": "Dữ liệu cửa sổ đã cũ so với sàn tham chiếu",
    "misaligned_window": "Cửa sổ dữ liệu lệch, không thể so sánh an toàn",
}


@dataclass(slots=True)
class VenueFlow:
    venue: str
    trade_count: int
    total_quote: float
    vwap: float
    window_start_ts: int
    window_end_ts: int


@dataclass(slots=True)
class VenueInputStatus:
    venue: str
    is_included: bool
    reason: str
    window_start_ts: int
    window_end_ts: int


@dataclass(slots=True)
class VenueComparisonResult:
    symbol: str
    market_type: str
    flows: list[VenueFlow]
    leader_venue: str
    lagger_venue: str
    vwap_spread_bps: float
    comparison_window_start_ts: int
    comparison_window_end_ts: int
    aligned_window_ms: int
    discovery_phase: str
    lead_score: float
    leader_confidence: float
    included_venues: list[str]
    excluded_venues: list[VenueInputStatus]


@dataclass(slots=True)
class _VenueBucket:
    venue: str
    trades: list[NormalizedTrade]
    window_start_ts: int
    window_end_ts: int


def compare_trade_flows(trades: list[NormalizedTrade]) -> VenueComparisonResult:
    if not trades:
        raise ValueError("trades must not be empty")

    _, symbol, market_type = parse_instrument_key(trades[0].instrument_key)
    venue_trades: dict[str, list[NormalizedTrade]] = {}
    for trade in trades:
        _, trade_symbol, trade_market_type = parse_instrument_key(trade.instrument_key)
        if trade_symbol != symbol or trade_market_type != market_type:
            continue
        venue_trades.setdefault(trade.venue, []).append(trade)

    buckets = [_build_bucket(venue, bucket_trades) for venue, bucket_trades in venue_trades.items()]
    if len(buckets) < 2:
        raise ValueError("at least two venues are required for comparison")

    reference_bucket = max(sorted(buckets, key=lambda item: item.venue), key=lambda item: item.window_end_ts)
    latest_window_end_ts = reference_bucket.window_end_ts
    flows: list[VenueFlow] = []
    excluded_venues: list[VenueInputStatus] = []

    for bucket in sorted(buckets, key=lambda item: item.venue):
        exclusion_reason = _resolve_exclusion_reason(
            bucket,
            reference_bucket=reference_bucket,
            latest_window_end_ts=latest_window_end_ts,
        )
        if exclusion_reason is not None:
            excluded_venues.append(
                VenueInputStatus(
                    venue=bucket.venue,
                    is_included=False,
                    reason=exclusion_reason,
                    window_start_ts=bucket.window_start_ts,
                    window_end_ts=bucket.window_end_ts,
                )
            )
            continue

        total_quote = sum(t.quote_qty for t in bucket.trades)
        total_qty = sum(t.qty for t in bucket.trades)
        vwap = total_quote / total_qty if total_quote > 0 and total_qty > 0 else 0.0
        flows.append(
            VenueFlow(
                venue=bucket.venue,
                trade_count=len(bucket.trades),
                total_quote=total_quote,
                vwap=vwap,
                window_start_ts=bucket.window_start_ts,
                window_end_ts=bucket.window_end_ts,
            )
        )

    if len(flows) < 2:
        raise ValueError("at least two aligned fresh venues are required for comparison")

    comparison_window_start_ts = max(flow.window_start_ts for flow in flows)
    comparison_window_end_ts = min(flow.window_end_ts for flow in flows)
    if comparison_window_end_ts < comparison_window_start_ts:
        comparison_window_start_ts = min(flow.window_end_ts for flow in flows)
        comparison_window_end_ts = comparison_window_start_ts
    aligned_window_ms = max(1, comparison_window_end_ts - comparison_window_start_ts + 1)

    flows.sort(key=lambda flow: flow.total_quote, reverse=True)
    leader = flows[0]
    lagger = flows[-1]

    max_vwap = max(flow.vwap for flow in flows)
    min_vwap = min(flow.vwap for flow in flows)
    vwap_spread_bps = ((max_vwap - min_vwap) / max_vwap) * 10_000 if max_vwap > 0 else 0.0
    total_quote = sum(flow.total_quote for flow in flows)
    runner_up_quote = flows[1].total_quote if len(flows) > 1 else 0.0
    lead_share = leader.total_quote / max(total_quote, 1.0)
    dominance_gap = max(0.0, leader.total_quote - runner_up_quote) / max(total_quote, 1.0)
    total_window_span_ms = max(1, max(flow.window_end_ts for flow in flows) - min(flow.window_start_ts for flow in flows))
    alignment_ratio = aligned_window_ms / total_window_span_ms
    cohesion_score = max(0.0, 1.0 - min(1.0, vwap_spread_bps / 12.0))
    leader_confidence = round(
        max(
            0.0,
            min(
                1.0,
                0.40 * lead_share + 0.30 * min(1.0, lead_share + dominance_gap) + 0.20 * alignment_ratio + 0.10 * cohesion_score,
            ),
        ),
        2,
    )
    if vwap_spread_bps > 5.0:
        discovery_phase = "DIVERGENT_DISCOVERY"
    elif leader_confidence >= 0.70:
        discovery_phase = "COHERENT_DISCOVERY"
    elif leader_confidence >= 0.50:
        discovery_phase = "COMPETITIVE_DISCOVERY"
    else:
        discovery_phase = "FRAGMENTED_DISCOVERY"

    return VenueComparisonResult(
        symbol=symbol,
        market_type=market_type,
        flows=flows,
        leader_venue=leader.venue,
        lagger_venue=lagger.venue,
        vwap_spread_bps=vwap_spread_bps,
        comparison_window_start_ts=comparison_window_start_ts,
        comparison_window_end_ts=comparison_window_end_ts,
        aligned_window_ms=aligned_window_ms,
        discovery_phase=discovery_phase,
        lead_score=leader_confidence,
        leader_confidence=leader_confidence,
        included_venues=[flow.venue for flow in flows],
        excluded_venues=excluded_venues,
    )



def _build_bucket(venue: str, trades: list[NormalizedTrade]) -> _VenueBucket:
    window_start_ts = min(trade.venue_ts for trade in trades)
    window_end_ts = max(trade.venue_ts for trade in trades)
    return _VenueBucket(
        venue=venue,
        trades=trades,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
    )



def _resolve_exclusion_reason(
    bucket: _VenueBucket,
    *,
    reference_bucket: _VenueBucket,
    latest_window_end_ts: int,
) -> str | None:
    if latest_window_end_ts - bucket.window_end_ts > MAX_VENUE_STALENESS_MS:
        return "stale_window"
    if bucket.window_end_ts < reference_bucket.window_start_ts - MAX_VENUE_WINDOW_SKEW_MS:
        return "misaligned_window"
    if bucket.window_start_ts > reference_bucket.window_end_ts + MAX_VENUE_WINDOW_SKEW_MS:
        return "misaligned_window"
    return None



def render_venue_comparison_vi(result: VenueComparisonResult) -> str:
    lines = [
        f"So sánh đa sàn cho {result.symbol} ({result.market_type})",
        f"Sàn dẫn nhịp theo quote volume: {result.leader_venue}",
        f"Sàn theo sau: {result.lagger_venue}",
        (
            "Cửa sổ so sánh hợp lệ: "
            f"{result.comparison_window_start_ts} -> {result.comparison_window_end_ts}"
        ),
        f"Overlap thực dùng để so sánh: {result.aligned_window_ms} ms",
        f"Độ lệch VWAP liên sàn: {result.vwap_spread_bps:.2f} bps",
        f"Độ tin cậy leader: {result.leader_confidence:.2f} | Phase: {result.discovery_phase}",
        "Chi tiết từng sàn:",
    ]
    for flow in result.flows:
        lines.append(
            (
                f"- {flow.venue}: {flow.trade_count} lệnh, quote={flow.total_quote:.2f}, "
                f"VWAP={flow.vwap:.2f}, cửa sổ={flow.window_start_ts}->{flow.window_end_ts}"
            )
        )
    if result.excluded_venues:
        lines.append("Sàn bị loại khỏi so sánh:")
        for status in result.excluded_venues:
            reason_label = _EXCLUSION_REASON_LABELS_VI.get(status.reason, status.reason)
            lines.append(
                f"- {status.venue}: {reason_label}, cửa sổ={status.window_start_ts}->{status.window_end_ts}"
            )
    return "\n".join(lines)


def build_venue_confirmation_context(
    result: VenueComparisonResult,
    *,
    primary_venue: str = "binance",
    max_confirmed_vwap_spread_bps: float = 5.0,
) -> dict[str, str | float]:
    primary = primary_venue.lower()
    included = {venue.lower() for venue in result.included_venues}
    leader = result.leader_venue.lower()
    lagger = result.lagger_venue.lower()

    if primary not in included:
        state = "UNCONFIRMED"
    elif result.vwap_spread_bps > max_confirmed_vwap_spread_bps:
        state = "DIVERGENT"
    elif result.leader_confidence < 0.45:
        state = "UNCONFIRMED"
    elif leader == primary:
        state = "CONFIRMED"
    else:
        state = "ALT_LEAD"

    return {
        "venue_confirmation_state": state,
        "leader_venue": leader,
        "lagger_venue": lagger,
        "venue_vwap_spread_bps": float(result.vwap_spread_bps),
        "discovery_phase": result.discovery_phase,
        "lead_score": result.lead_score,
        "leader_confidence": result.leader_confidence,
        "aligned_window_ms": int(result.aligned_window_ms),
    }
