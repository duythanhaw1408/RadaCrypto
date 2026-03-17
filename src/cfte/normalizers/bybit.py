from __future__ import annotations

import hashlib
import json
from typing import Any

from cfte.models.events import NormalizedBookTop, NormalizedTrade

BYBIT_VENUE = "bybit"


def _event_id(prefix: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    raw = f"{prefix}:{canonical}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def normalize_public_trade(trade: dict[str, Any], instrument_key: str) -> NormalizedTrade:
    side = str(trade["S"]).upper()
    taker_side = "BUY" if side == "BUY" else "SELL"
    price = float(trade["p"])
    qty = float(trade["v"])
    return NormalizedTrade(
        event_id=_event_id("publicTrade", trade),
        venue=BYBIT_VENUE,
        instrument_key=instrument_key,
        price=price,
        qty=qty,
        quote_qty=price * qty,
        taker_side=taker_side,
        venue_ts=int(trade["T"]),
    )


def normalize_orderbook_top(msg: dict[str, Any], instrument_key: str) -> NormalizedBookTop:
    data = msg["data"]
    best_bid = data["b"][0]
    best_ask = data["a"][0]
    return NormalizedBookTop(
        event_id=_event_id("orderbook", msg),
        venue=BYBIT_VENUE,
        instrument_key=instrument_key,
        bid_px=float(best_bid[0]),
        bid_qty=float(best_bid[1]),
        ask_px=float(best_ask[0]),
        ask_qty=float(best_ask[1]),
        venue_ts=int(data.get("cts", msg.get("ts", 0))),
    )
