from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from cfte.models.events import NormalizedDepthDiff, NormalizedTrade

ReplayEventType = Literal["book_snapshot", "depth_diff", "trade"]


@dataclass(slots=True)
class ReplayBookSnapshot:
    instrument_key: str
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    seq_id: int
    venue_ts: int


@dataclass(slots=True)
class ReplayEvent:
    event_type: ReplayEventType
    venue_ts: int
    payload: ReplayBookSnapshot | NormalizedDepthDiff | NormalizedTrade


def _to_levels(raw: list[list[float]] | list[tuple[float, float]]) -> list[tuple[float, float]]:
    return [(float(px), float(qty)) for px, qty in raw]


def _parse_event(line: str) -> ReplayEvent:
    record = json.loads(line)
    event_type = str(record["event_type"])

    if event_type == "book_snapshot":
        payload = ReplayBookSnapshot(
            instrument_key=str(record["instrument_key"]),
            bids=_to_levels(record["bids"]),
            asks=_to_levels(record["asks"]),
            seq_id=int(record["seq_id"]),
            venue_ts=int(record["venue_ts"]),
        )
        return ReplayEvent(event_type="book_snapshot", venue_ts=payload.venue_ts, payload=payload)

    if event_type == "depth_diff":
        payload = NormalizedDepthDiff(
            event_id=str(record["event_id"]),
            venue=str(record["venue"]),
            instrument_key=str(record["instrument_key"]),
            first_update_id=int(record["first_update_id"]),
            final_update_id=int(record["final_update_id"]),
            bid_updates=_to_levels(record.get("bid_updates", [])),
            ask_updates=_to_levels(record.get("ask_updates", [])),
            venue_ts=int(record["venue_ts"]),
        )
        return ReplayEvent(event_type="depth_diff", venue_ts=payload.venue_ts, payload=payload)

    if event_type == "trade":
        payload = NormalizedTrade(
            event_id=str(record["event_id"]),
            venue=str(record["venue"]),
            instrument_key=str(record["instrument_key"]),
            price=float(record["price"]),
            qty=float(record["qty"]),
            quote_qty=float(record["quote_qty"]),
            taker_side=str(record["taker_side"]),
            venue_ts=int(record["venue_ts"]),
        )
        return ReplayEvent(event_type="trade", venue_ts=payload.venue_ts, payload=payload)

    raise ValueError(f"Unsupported replay event_type: {event_type}")


def load_replay_events(path: str | Path) -> list[ReplayEvent]:
    source = Path(path)
    events: list[tuple[int, ReplayEvent]] = []
    with source.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            events.append((idx, _parse_event(line)))
    events.sort(key=lambda item: (item[1].venue_ts, item[0]))
    return [event for _, event in events]
