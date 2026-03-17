from __future__ import annotations

import hashlib
from typing import Any

from cfte.models.events import NormalizedBookTop, NormalizedTrade

def _event_id(prefix: str, payload: dict[str, Any]) -> str:
    raw = f"{prefix}:{payload}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()

def normalize_agg_trade(msg: dict[str, Any], instrument_key: str) -> NormalizedTrade:
    taker_side = "SELL" if msg["m"] else "BUY"
    return NormalizedTrade(
        event_id=_event_id("aggTrade", msg),
        venue="binance",
        instrument_key=instrument_key,
        price=float(msg["p"]),
        qty=float(msg["q"]),
        quote_qty=float(msg.get("p", 0.0)) * float(msg.get("q", 0.0)),
        taker_side=taker_side,
        venue_ts=int(msg["T"]),
    )

def normalize_book_ticker(msg: dict[str, Any], instrument_key: str) -> NormalizedBookTop:
    return NormalizedBookTop(
        event_id=_event_id("bookTicker", msg),
        venue="binance",
        instrument_key=instrument_key,
        bid_px=float(msg["b"]),
        bid_qty=float(msg["B"]),
        ask_px=float(msg["a"]),
        ask_qty=float(msg["A"]),
        venue_ts=int(msg["E"]),
    )
