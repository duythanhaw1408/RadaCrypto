from __future__ import annotations

from cfte.books.local_book import LocalBook
from cfte.models.events import NormalizedTrade, TapeSnapshot

MIN_PRICE_BPS_DENOM = 0.01
MIN_WINDOW_SECONDS = 1.0
DEFAULT_TRADE_WINDOW_SECONDS = 60.0
DEFAULT_RECENT_FLOW_SECONDS = 15.0
DEFAULT_MAX_WINDOW_TRADES = 400


def get_rolling_window_trades(
    trades: list[NormalizedTrade],
    duration_seconds: float,
    end_ts: int
) -> list[NormalizedTrade]:
    """Trả về các trade trong cửa sổ lùi từ end_ts (millisecond)."""
    cutoff = end_ts - (duration_seconds * 1000)
    # Giả định trades đã sắp xếp theo venue_ts
    return [t for t in trades if t.venue_ts >= cutoff]


def slice_trade_window(
    trades: list[NormalizedTrade],
    *,
    end_ts: int,
    lookback_seconds: float = DEFAULT_TRADE_WINDOW_SECONDS,
    max_trades: int | None = DEFAULT_MAX_WINDOW_TRADES,
) -> list[NormalizedTrade]:
    windowed = get_rolling_window_trades(trades, duration_seconds=lookback_seconds, end_ts=end_ts)
    if max_trades is not None and len(windowed) > max_trades:
        return windowed[-max_trades:]
    return windowed


def delta_quote(trades: list[NormalizedTrade]) -> float:
    value = 0.0
    for t in trades:
        sign = 1.0 if t.taker_side == "BUY" else -1.0
        value += sign * t.quote_qty
    return value

def cvd(trades: list[NormalizedTrade], previous_cvd: float = 0.0) -> float:
    return previous_cvd + delta_quote(trades)

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
    denom = max(abs(price_change_bps), MIN_PRICE_BPS_DENOM)
    return traded_quote / denom


def detect_sweeps(trades: list[NormalizedTrade]) -> float:
    """
    Detects 'sweeps' by grouping trades by timestamp and checking if 
    a single timestamp has trades at multiple price levels.
    """
    if not trades:
        return 0.0
    
    ts_map: dict[int, set[float]] = {}
    ts_vol: dict[int, float] = {}
    
    for t in trades:
        ts_map.setdefault(t.venue_ts, set()).add(t.price)
        ts_vol[t.venue_ts] = ts_vol.get(t.venue_ts, 0.0) + t.quote_qty
        
    sweep_vol = 0.0
    for ts, prices in ts_map.items():
        if len(prices) > 1:
            sweep_vol += ts_vol[ts]
            
    return sweep_vol


def detect_directional_sweeps(trades: list[NormalizedTrade]) -> tuple[float, float]:
    """Returns sweep quote split by aggressive buy/sell direction."""
    if not trades:
        return 0.0, 0.0

    ts_prices: dict[int, set[float]] = {}
    ts_buy_quote: dict[int, float] = {}
    ts_sell_quote: dict[int, float] = {}

    for trade in trades:
        ts_prices.setdefault(trade.venue_ts, set()).add(trade.price)
        if trade.taker_side == "BUY":
            ts_buy_quote[trade.venue_ts] = ts_buy_quote.get(trade.venue_ts, 0.0) + trade.quote_qty
        else:
            ts_sell_quote[trade.venue_ts] = ts_sell_quote.get(trade.venue_ts, 0.0) + trade.quote_qty

    sweep_buy_quote = 0.0
    sweep_sell_quote = 0.0
    for ts, prices in ts_prices.items():
        if len(prices) <= 1:
            continue
        sweep_buy_quote += ts_buy_quote.get(ts, 0.0)
        sweep_sell_quote += ts_sell_quote.get(ts, 0.0)

    return sweep_buy_quote, sweep_sell_quote


def recent_quote_share(
    trades: list[NormalizedTrade],
    *,
    end_ts: int,
    duration_seconds: float = DEFAULT_RECENT_FLOW_SECONDS,
) -> float:
    if not trades:
        return 0.0

    total_quote = sum(t.quote_qty for t in trades)
    if total_quote <= 0:
        return 0.0

    recent_trades = get_rolling_window_trades(trades, duration_seconds=duration_seconds, end_ts=end_ts)
    recent_quote = sum(t.quote_qty for t in recent_trades)
    return recent_quote / total_quote


def burst_persistence(trades: list[NormalizedTrade], *, window_seconds: float) -> float:
    """Scores whether burst activity persists through the window instead of clustering in one burst."""
    if not trades or window_seconds <= 0:
        return 0.0

    bucket_count = 4 if window_seconds >= 20 else 3 if window_seconds >= 10 else 2
    start_ts = min(t.venue_ts for t in trades)
    bucket_ms = max(int((window_seconds * 1000) / bucket_count), 1)
    bucket_quote = [0.0 for _ in range(bucket_count)]

    total_quote = sum(t.quote_qty for t in trades)
    if total_quote <= 0:
        return 0.0

    for trade in trades:
        bucket_idx = min(bucket_count - 1, max(0, int((trade.venue_ts - start_ts) / bucket_ms)))
        bucket_quote[bucket_idx] += trade.quote_qty

    active_ratio = sum(1 for quote in bucket_quote if quote > 0) / bucket_count
    concentration = max(bucket_quote) / total_quote
    persistence = 0.6 * active_ratio + 0.4 * (1.0 - concentration)
    return max(0.0, min(1.0, persistence))


def microprice_drift_bps(
    trades: list[NormalizedTrade],
    *,
    order_book: LocalBook,
    before_book: LocalBook | None = None,
) -> float:
    current_micro = microprice(order_book)
    if before_book is not None:
        anchor_px = microprice(before_book)
    elif trades:
        anchor_px = trades[0].price
    else:
        return 0.0

    if anchor_px <= 0:
        return 0.0
    return ((current_micro - anchor_px) / anchor_px) * 10000.0

def replenishment_score(
    trades: list[NormalizedTrade], 
    before_book: LocalBook, 
    after_book: LocalBook
) -> tuple[float, float]:
    """
    Detects bid/ask replenishment. 
    Returns (bid_replenishment, ask_replenishment) scores.
    """
    # Simple logic: if bid quantity after > bid quantity before - trade volume at bid
    # and bid price hasn't changed.
    bid_before_px, bid_before_qty = before_book.best_bid()
    bid_after_px, bid_after_qty = after_book.best_bid()
    
    ask_before_px, ask_before_qty = before_book.best_ask()
    ask_after_px, ask_after_qty = after_book.best_ask()
    
    buy_quote = sum(t.quote_qty for t in trades if t.taker_side == "BUY")
    sell_quote = sum(t.quote_qty for t in trades if t.taker_side == "SELL")
    
    bid_replenishment = 0.0
    if bid_before_px == bid_after_px and sell_quote > 0:
        expected_bid_qty = max(0, bid_before_qty - (sell_quote / max(bid_before_px, 1.0)))
        if bid_after_qty > expected_bid_qty:
            bid_replenishment = (bid_after_qty - expected_bid_qty) * bid_after_px
            
    ask_replenishment = 0.0
    if ask_before_px == ask_after_px and buy_quote > 0:
        expected_ask_qty = max(0, ask_before_qty - (buy_quote / max(ask_before_px, 1.0)))
        if ask_after_qty > expected_ask_qty:
            ask_replenishment = (ask_after_qty - expected_ask_qty) * ask_after_px
            
    return bid_replenishment, ask_replenishment


def build_tape_snapshot(
    instrument_key: str,
    order_book: LocalBook,
    trades: list[NormalizedTrade],
    window_start_ts: int | None = None,
    window_end_ts: int | None = None,
    previous_cvd: float = 0.0,
    lookback_seconds: float | None = None,
    max_window_trades: int | None = DEFAULT_MAX_WINDOW_TRADES,
    futures_delta: float = 0.0,
    liquidation_vol: float = 0.0,
    liquidation_bias: str = "NEUTRAL",
    venue_confirmation_state: str = "UNCONFIRMED",
    leader_venue: str = "UNKNOWN",
    before_book: LocalBook | None = None,
) -> TapeSnapshot:
    if not trades:
        # Fallback if no trades
        bid_px, _ = order_book.best_bid()
        ask_px, _ = order_book.best_ask()
        mid_px = (bid_px + ask_px) / 2.0
        now = window_end_ts or 0
        return TapeSnapshot(
            instrument_key=instrument_key,
            window_start_ts=now,
            window_end_ts=now,
            spread_bps=order_book.spread_bps(),
            microprice=microprice(order_book),
            imbalance_l1=order_book.imbalance_l1(),
            delta_quote=0.0,
            cvd=previous_cvd,
            trade_burst=0.0,
            absorption_proxy=0.0,
            bid_px=bid_px,
            ask_px=ask_px,
            mid_px=mid_px,
            last_trade_px=mid_px,
            trade_count=0,
            futures_delta=futures_delta,
            liquidation_vol=liquidation_vol,
            liquidation_bias=liquidation_bias,
            venue_confirmation_state=venue_confirmation_state,
            leader_venue=leader_venue,
        )

    actual_end = window_end_ts or trades[-1].venue_ts
    
    if lookback_seconds:
        active_trades = slice_trade_window(
            trades,
            end_ts=actual_end,
            lookback_seconds=lookback_seconds,
            max_trades=max_window_trades,
        )
        actual_start = active_trades[0].venue_ts if active_trades else actual_end - int(lookback_seconds * 1000)
    else:
        active_trades = trades
        actual_start = window_start_ts or trades[0].venue_ts

    bid_px, _ = order_book.best_bid()
    ask_px, _ = order_book.best_ask()
    mid_px = (bid_px + ask_px) / 2.0
    last_trade_px = trades[-1].price
    price_change_bps = ((last_trade_px - mid_px) / mid_px) * 10000.0
    window_seconds = max((actual_end - actual_start) / 1000.0, MIN_WINDOW_SECONDS)
    
    delta_quote_value = delta_quote(active_trades)
    cvd_value = cvd(active_trades, previous_cvd=previous_cvd)
    freshness_share = recent_quote_share(active_trades, end_ts=actual_end)
    total_quote = sum(t.quote_qty for t in active_trades)
    buy_quote = sum(t.quote_qty for t in active_trades if t.taker_side == "BUY")
    aggression_ratio_value = buy_quote / max(total_quote, 1.0)
    sweep_buy_quote, sweep_sell_quote = detect_directional_sweeps(active_trades)
    burst_persistence_value = burst_persistence(active_trades, window_seconds=window_seconds)
    microprice_drift_value = microprice_drift_bps(
        active_trades,
        order_book=order_book,
        before_book=before_book,
    )

    bid_replen, ask_replen = 0.0, 0.0
    if before_book:
        bid_replen, ask_replen = replenishment_score(active_trades, before_book, order_book)

    return TapeSnapshot(
        instrument_key=instrument_key,
        window_start_ts=actual_start,
        window_end_ts=actual_end,
        spread_bps=order_book.spread_bps(),
        microprice=microprice(order_book),
        imbalance_l1=order_book.imbalance_l1(),
        delta_quote=delta_quote_value,
        cvd=cvd_value,
        trade_burst=trade_burst(active_trades, window_seconds=window_seconds),
        absorption_proxy=absorption_proxy(active_trades, price_change_bps=price_change_bps),
        bid_px=bid_px,
        ask_px=ask_px,
        mid_px=mid_px,
        last_trade_px=last_trade_px,
        trade_count=len(active_trades),
        futures_delta=futures_delta,
        liquidation_vol=liquidation_vol,
        liquidation_bias=liquidation_bias,
        venue_confirmation_state=venue_confirmation_state,
        leader_venue=leader_venue,
        metadata={
            "window_seconds": window_seconds,
            "price_change_bps": price_change_bps,
            "is_rolling": lookback_seconds is not None,
            "recent_quote_share": freshness_share,
            "recent_flow_seconds": min(DEFAULT_RECENT_FLOW_SECONDS, window_seconds),
            "window_trade_count": len(active_trades),
            "window_quote_total": total_quote,
            "aggression_ratio": aggression_ratio_value,
            "bid_replenishment": bid_replen,
            "ask_replenishment": ask_replen,
            "replenishment_bid_score": bid_replen,
            "replenishment_ask_score": ask_replen,
            "sweep_quote": detect_sweeps(active_trades),
            "sweep_buy_quote": sweep_buy_quote,
            "sweep_sell_quote": sweep_sell_quote,
            "burst_persistence": burst_persistence_value,
            "microprice_drift_bps": microprice_drift_value,
        },
    )
