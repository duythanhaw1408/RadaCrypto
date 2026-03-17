from __future__ import annotations

import hashlib
import json
from typing import Any

from cfte.models.events import NormalizedBookTop, NormalizedDepthDiff, NormalizedKline, NormalizedTrade

BINANCE_VENUE = "binance"


def _event_id(prefix: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    raw = f"{prefix}:{canonical}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def _quote_qty(price: Any, qty: Any) -> float:
    return float(price) * float(qty)


def normalize_agg_trade(msg: dict[str, Any], instrument_key: str) -> NormalizedTrade:
    taker_side = "SELL" if msg["m"] else "BUY"
    return NormalizedTrade(
        event_id=_event_id("aggTrade", msg),
        venue=BINANCE_VENUE,
        instrument_key=instrument_key,
        price=float(msg["p"]),
        qty=float(msg["q"]),
        quote_qty=_quote_qty(msg["p"], msg["q"]),
        taker_side=taker_side,
        venue_ts=int(msg["T"]),
    )


def normalize_trade(msg: dict[str, Any], instrument_key: str) -> NormalizedTrade:
    taker_side = "SELL" if msg["m"] else "BUY"
    return NormalizedTrade(
        event_id=_event_id("trade", msg),
        venue=BINANCE_VENUE,
        instrument_key=instrument_key,
        price=float(msg["p"]),
        qty=float(msg["q"]),
        quote_qty=_quote_qty(msg["p"], msg["q"]),
        taker_side=taker_side,
        venue_ts=int(msg["T"]),
    )


def normalize_book_ticker(msg: dict[str, Any], instrument_key: str) -> NormalizedBookTop:
    return NormalizedBookTop(
        event_id=_event_id("bookTicker", msg),
        venue=BINANCE_VENUE,
        instrument_key=instrument_key,
        bid_px=float(msg["b"]),
        bid_qty=float(msg["B"]),
        ask_px=float(msg["a"]),
        ask_qty=float(msg["A"]),
        venue_ts=int(msg["E"]),
    )


def normalize_depth_diff(msg: dict[str, Any], instrument_key: str) -> NormalizedDepthDiff:
    return NormalizedDepthDiff(
        event_id=_event_id("depth", msg),
        venue=BINANCE_VENUE,
        instrument_key=instrument_key,
        first_update_id=int(msg["U"]),
        final_update_id=int(msg["u"]),
        bid_updates=[(float(px), float(qty)) for px, qty in msg.get("b", [])],
        ask_updates=[(float(px), float(qty)) for px, qty in msg.get("a", [])],
        venue_ts=int(msg["E"]),
    )


def normalize_kline(msg: dict[str, Any], instrument_key: str) -> NormalizedKline:
    kline = msg["k"]
    return NormalizedKline(
        event_id=_event_id("kline", msg),
        venue=BINANCE_VENUE,
        instrument_key=instrument_key,
        interval=str(kline["i"]),
        open_px=float(kline["o"]),
        high_px=float(kline["h"]),
        low_px=float(kline["l"]),
        close_px=float(kline["c"]),
        base_volume=float(kline["v"]),
        quote_volume=float(kline["q"]),
        open_ts=int(kline["t"]),
        close_ts=int(kline["T"]),
        is_closed=bool(kline["x"]),
        venue_ts=int(msg["E"]),
    )
