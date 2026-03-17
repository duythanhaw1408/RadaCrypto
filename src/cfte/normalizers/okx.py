from __future__ import annotations

import hashlib
import json
from typing import Any

from cfte.models.events import NormalizedBookTop, NormalizedTrade

OKX_VENUE = "okx"


def _event_id(prefix: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    raw = f"{prefix}:{canonical}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def normalize_trade(trade: dict[str, Any], instrument_key: str) -> NormalizedTrade:
    side = str(trade["side"]).upper()
    taker_side = "BUY" if side == "BUY" else "SELL"
    price = float(trade["px"])
    qty = float(trade["sz"])
    return NormalizedTrade(
        event_id=_event_id("trades", trade),
        venue=OKX_VENUE,
        instrument_key=instrument_key,
        price=price,
        qty=qty,
        quote_qty=price * qty,
        taker_side=taker_side,
        venue_ts=int(trade["ts"]),
    )


def normalize_bbo_tbt(book: dict[str, Any], instrument_key: str) -> NormalizedBookTop:
    bid = book["bids"][0]
    ask = book["asks"][0]
    return NormalizedBookTop(
        event_id=_event_id("bbo-tbt", book),
        venue=OKX_VENUE,
        instrument_key=instrument_key,
        bid_px=float(bid[0]),
        bid_qty=float(bid[1]),
        ask_px=float(ask[0]),
        ask_qty=float(ask[1]),
        venue_ts=int(book["ts"]),
    )
