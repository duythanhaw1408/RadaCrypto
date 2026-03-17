from __future__ import annotations

from cfte.books.local_book import LocalBook
from cfte.models.events import NormalizedTrade, TapeSnapshot

def delta_quote(trades: list[NormalizedTrade]) -> float:
    value = 0.0
    for t in trades:
        sign = 1.0 if t.taker_side == "BUY" else -1.0
        value += sign * t.quote_qty
    return value

def cvd(trades: list[NormalizedTrade]) -> float:
    return delta_quote(trades)

def trade_burst(trades: list[NormalizedTrade], window_seconds: float) -> float:
    if window_seconds <= 0:
        return 0.0
    return len(trades) / window_seconds

def microprice(book: LocalBook) -> float:
    bid_px, bid_qty = book.best_bid()
    ask_px, ask_qty = book.best_ask()
    total = bid_qty + ask_qty
    if total == 0:
        return (bid_px + ask_px) / 2.0
    return (ask_px * bid_qty + bid_px * ask_qty) / total

def absorption_proxy(trades: list[NormalizedTrade], price_change_bps: float) -> float:
    traded_quote = sum(t.quote_qty for t in trades)
    denom = max(abs(price_change_bps), 0.01)
    return traded_quote / denom

def build_tape_snapshot(
    instrument_key: str,
    order_book: LocalBook,
    trades: list[NormalizedTrade],
    window_start_ts: int,
    window_end_ts: int,
) -> TapeSnapshot:
    bid_px, _ = order_book.best_bid()
    ask_px, _ = order_book.best_ask()
    mid_px = (bid_px + ask_px) / 2.0
    last_trade_px = trades[-1].price if trades else mid_px
    price_change_bps = ((last_trade_px - mid_px) / mid_px) * 10000.0
    window_seconds = max((window_end_ts - window_start_ts) / 1000.0, 1.0)
    return TapeSnapshot(
        instrument_key=instrument_key,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        spread_bps=order_book.spread_bps(),
        microprice=microprice(order_book),
        imbalance_l1=order_book.imbalance_l1(),
        delta_quote=delta_quote(trades),
        cvd=cvd(trades),
        trade_burst=trade_burst(trades, window_seconds=window_seconds),
        absorption_proxy=absorption_proxy(trades, price_change_bps=price_change_bps),
        bid_px=bid_px,
        ask_px=ask_px,
        mid_px=mid_px,
        last_trade_px=last_trade_px,
        trade_count=len(trades),
        metadata={},
    )
