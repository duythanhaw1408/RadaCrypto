from __future__ import annotations

import asyncio
import json
import ssl
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, Optional

import certifi
import requests

from cfte.collectors.health import CollectorErrorSurface, CollectorHealthSnapshot, CollectorState, build_error_surface


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def summarize_futures_agg_trades(rows: Iterable[dict[str, Any]]) -> Dict[str, Any]:
    buy_quote = 0.0
    sell_quote = 0.0
    trade_count = 0
    last_trade_ts = 0

    for row in rows:
        price = _to_float(row.get("p"))
        qty = _to_float(row.get("q"))
        quote_qty = _to_float(row.get("quote_qty"), price * qty)
        if quote_qty <= 0:
            continue

        trade_count += 1
        if bool(row.get("m", False)):
            sell_quote += quote_qty
        else:
            buy_quote += quote_qty
        last_trade_ts = max(last_trade_ts, _to_int(row.get("ts", row.get("T", row.get("E")))))

    total_quote = buy_quote + sell_quote
    return {
        "available": trade_count > 0,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "total_quote": total_quote,
        "delta_quote": buy_quote - sell_quote,
        "aggression_ratio": buy_quote / max(total_quote, 1.0),
        "trade_count": trade_count,
        "last_trade_ts": last_trade_ts,
    }


def summarize_force_orders(rows: Iterable[dict[str, Any]]) -> Dict[str, Any]:
    buy_quote = 0.0
    sell_quote = 0.0
    liquidation_count = 0
    last_liquidation_ts = 0

    for row in rows:
        order = row.get("o", row)
        side = str(order.get("S", order.get("side", ""))).upper()
        price = _to_float(order.get("ap"), _to_float(order.get("p"), _to_float(order.get("price"))))
        qty = _to_float(order.get("z"), _to_float(order.get("q"), _to_float(order.get("origQty"))))
        quote_qty = _to_float(order.get("quote_qty"), price * qty)
        if quote_qty <= 0:
            continue

        liquidation_count += 1
        if side == "BUY":
            buy_quote += quote_qty
        elif side == "SELL":
            sell_quote += quote_qty
        last_liquidation_ts = max(last_liquidation_ts, _to_int(order.get("T", row.get("E", row.get("ts")))))

    if liquidation_count == 0:
        bias = "NONE"
    elif sell_quote > buy_quote * 1.2:
        bias = "LONGS_FLUSHED"
    elif buy_quote > sell_quote * 1.2:
        bias = "SHORTS_FLUSHED"
    else:
        bias = "MIXED"

    total_quote = buy_quote + sell_quote
    return {
        "available": liquidation_count > 0,
        "bias": bias,
        "count": liquidation_count,
        "quote": total_quote,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "last_liquidation_ts": last_liquidation_ts,
    }


def classify_basis_state(basis_bps: float) -> str:
    if basis_bps >= 12.0:
        return "OVERHEATED_PREMIUM"
    if basis_bps >= 4.0:
        return "PREMIUM"
    if basis_bps <= -12.0:
        return "DEEP_DISCOUNT"
    if basis_bps <= -4.0:
        return "DISCOUNT"
    return "BALANCED"


class BinanceFuturesCollector:
    """Collector for Binance Futures context with rolling delta and liquidation windows."""

    REST_MIRRORS = [
        "https://fapi.binance.com",
        "https://fapi-gcp.binance.com",
        "https://fapi1.binance.com",
        "https://fapi2.binance.com",
        "https://fapi3.binance.com",
        "https://fapi.binance.me",
    ]
    
    WS_MIRRORS = [
        "wss://fstream.binance.com/stream",
        "wss://fstream-gcp.binance.com/stream",
        "wss://fstream1.binance.com/stream",
        "wss://fstream2.binance.com/stream",
        "wss://fstream3.binance.com/stream",
    ]

    REST_BASE = REST_MIRRORS[0]
    WS_BASE = WS_MIRRORS[0]

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        *,
        context_window_seconds: float = 30.0,
        rest_refresh_seconds: float = 3.0,
        max_trade_events: int = 2000,
        max_liquidation_events: int = 400,
    ) -> None:
        self.symbol = symbol.upper()
        self.context_window_seconds = max(5.0, float(context_window_seconds))
        self.rest_refresh_seconds = max(1.0, float(rest_refresh_seconds))
        self._last_oi: Optional[float] = None
        self._last_ts: Optional[int] = None
        self._rest_context_cache: dict[str, Any] = {}
        self._rest_context_ts: int = 0
        self._agg_trade_rows: Deque[dict[str, Any]] = deque(maxlen=max_trade_events)
        self._force_order_rows: Deque[dict[str, Any]] = deque(maxlen=max_liquidation_events)
        self._state: CollectorState = "idle"
        self._connected = False
        self._connect_attempts = 0
        self._reconnect_count = 0
        self._message_count = 0
        self._last_disconnect_reason: CollectorErrorSurface | None = None
        self._last_error: CollectorErrorSurface | None = None
        self._stream_connected = False
        self._last_ws_message_ts: Optional[int] = None
        self._last_agg_trade_ts: Optional[int] = None
        self._last_force_order_ts: Optional[int] = None
        self._seeded_recent_trades = False

    def fetch_mark_price_info(self) -> Dict[str, Any]:
        for mirror in self.REST_MIRRORS:
            url = f"{mirror}/fapi/v1/premiumIndex"
            params = {"symbol": self.symbol}
            try:
                resp = requests.get(url, params=params, timeout=5)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                if hasattr(exc, 'response') and exc.response is not None and exc.response.status_code == 451:
                    continue
                print(f"⚠️ Error fetching Binance Futures Premium Index on {mirror}: {exc}")
                break
        return {}

    def fetch_open_interest(self) -> Dict[str, Any]:
        for mirror in self.REST_MIRRORS:
            url = f"{mirror}/fapi/v1/openInterest"
            params = {"symbol": self.symbol}
            try:
                resp = requests.get(url, params=params, timeout=5)
                resp.raise_for_status()
                return resp.json()
            except Exception:
                continue
        return {}

    def fetch_recent_agg_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        for mirror in self.REST_MIRRORS:
            url = f"{mirror}/fapi/v1/aggTrades"
            params = {"symbol": self.symbol, "limit": max(1, min(int(limit), 1000))}
            try:
                resp = requests.get(url, params=params, timeout=5)
                resp.raise_for_status()
                return resp.json()
            except Exception:
                continue
        return []

    def fetch_recent_force_orders(self, limit: int = 50, start_time_ms: int | None = None) -> list[dict[str, Any]]:
        for mirror in self.REST_MIRRORS:
            url = f"{mirror}/fapi/v1/allForceOrders"
            params: dict[str, Any] = {"symbol": self.symbol, "limit": max(1, min(int(limit), 100))}
            if start_time_ms is not None:
                params["startTime"] = int(start_time_ms)
            try:
                resp = requests.get(url, params=params, timeout=5)
                resp.raise_for_status()
                return resp.json()
            except Exception:
                continue
        return []

    def _append_agg_trade(self, trade: dict[str, Any]) -> None:
        ts = _to_int(trade.get("T", trade.get("E", trade.get("ts"))))
        price = _to_float(trade.get("p"))
        qty = _to_float(trade.get("q"))
        quote_qty = _to_float(trade.get("quote_qty"), price * qty)
        if ts <= 0 or price <= 0 or qty <= 0 or quote_qty <= 0:
            return
        self._agg_trade_rows.append(
            {
                "ts": ts,
                "p": price,
                "q": qty,
                "quote_qty": quote_qty,
                "m": bool(trade.get("m", False)),
            }
        )

    def _append_force_order(self, payload: dict[str, Any]) -> None:
        order = payload.get("o", payload)
        ts = _to_int(order.get("T", payload.get("E", payload.get("ts"))))
        price = _to_float(order.get("ap"), _to_float(order.get("p"), _to_float(order.get("price"))))
        qty = _to_float(order.get("z"), _to_float(order.get("q"), _to_float(order.get("origQty"))))
        quote_qty = _to_float(order.get("quote_qty"), price * qty)
        side = str(order.get("S", order.get("side", ""))).upper()
        if ts <= 0 or price <= 0 or qty <= 0 or quote_qty <= 0 or side not in {"BUY", "SELL"}:
            return
        self._force_order_rows.append(
            {
                "o": {
                    "T": ts,
                    "S": side,
                    "ap": price,
                    "q": qty,
                    "quote_qty": quote_qty,
                }
            }
        )

    def _trim_rows(self, rows: Deque[dict[str, Any]], *, now_ms: int) -> None:
        cutoff = now_ms - int(self.context_window_seconds * 1000)
        while rows and _to_int(rows[0].get("ts", rows[0].get("o", {}).get("T"))) < cutoff:
            rows.popleft()

    def recent_agg_trade_rows(self, *, now_ms: int | None = None) -> list[dict[str, Any]]:
        current_ts = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        self._trim_rows(self._agg_trade_rows, now_ms=current_ts)
        return list(self._agg_trade_rows)

    def recent_force_orders(self, *, now_ms: int | None = None) -> list[dict[str, Any]]:
        current_ts = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        self._trim_rows(self._force_order_rows, now_ms=current_ts)
        return list(self._force_order_rows)

    def _build_rest_context(self, *, now_ms: int) -> dict[str, Any]:
        refresh_ms = int(self.rest_refresh_seconds * 1000)
        cache_age_ms = now_ms - self._rest_context_ts
        if self._rest_context_cache and cache_age_ms <= refresh_ms:
            cached = dict(self._rest_context_cache)
            cached["fresh"] = True
            return cached

        mark_info = self.fetch_mark_price_info()
        oi_info = self.fetch_open_interest()
        if not mark_info or not oi_info:
            if self._rest_context_cache:
                cached = dict(self._rest_context_cache)
                cached["fresh"] = False
                return cached
            return {}

        current_oi = _to_float(oi_info.get("openInterest"))
        previous_oi = self._last_oi
        oi_delta = 0.0 if previous_oi is None else current_oi - previous_oi
        oi_expansion_ratio = 0.0 if previous_oi in {None, 0.0} else oi_delta / max(abs(previous_oi), 1.0)
        self._last_oi = current_oi
        self._last_ts = now_ms

        mark_px = _to_float(mark_info.get("markPrice"))
        index_px = _to_float(mark_info.get("indexPrice"))
        basis_bps = 0.0
        if index_px > 0:
            basis_bps = ((mark_px - index_px) / index_px) * 10000.0

        context = {
            "available": True,
            "fresh": True,
            "timestamp": now_ms,
            "mark_price": mark_px,
            "index_price": index_px,
            "basis_bps": basis_bps,
            "basis_state": classify_basis_state(basis_bps),
            "funding_rate": _to_float(mark_info.get("lastFundingRate")),
            "oi_value": current_oi,
            "oi_delta": oi_delta,
            "oi_expansion_ratio": oi_expansion_ratio,
        }
        self._rest_context_cache = context
        self._rest_context_ts = now_ms
        return dict(context)

    def _seed_recent_trades_once(self) -> None:
        if self._seeded_recent_trades:
            return
        self._seeded_recent_trades = True
        for row in self.fetch_recent_agg_trades(limit=min(self._agg_trade_rows.maxlen, 200)):
            self._append_agg_trade(row)

    async def stream_forever(self) -> None:
        import websockets

        streams = [
            f"{self.symbol.lower()}@aggTrade",
            f"{self.symbol.lower()}@forceOrder",
        ]
        url = f"{self.WS_BASE}?streams={'/'.join(streams)}"
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        mirror_idx = 0
        while True:
            try:
                self._connect_attempts += 1
                current_url = f"{self.WS_MIRRORS[mirror_idx % len(self.WS_MIRRORS)]}?streams={'/'.join(streams)}"
                async with websockets.connect(current_url, ssl=ssl_context) as ws:
                    self._connected = True
                    self._stream_connected = True
                    self._state = "running"
                    self._last_error = None
                    print(f"📡 Futures Stream Connected: {current_url}")
                    async for raw in ws:
                        self._message_count += 1
                        envelope = json.loads(raw)
                        data = envelope.get("data", {})
                        event_type = data.get("e")
                        self._last_ws_message_ts = _to_int(data.get("E"), int(datetime.now(timezone.utc).timestamp() * 1000))

                        if event_type == "aggTrade":
                            self._append_agg_trade(data)
                            self._last_agg_trade_ts = self._last_ws_message_ts
                        elif event_type == "forceOrder":
                            self._append_force_order(data)
                            self._last_force_order_ts = self._last_ws_message_ts
            except Exception as exc:
                error = build_error_surface(exc)
                self._connected = False
                self._stream_connected = False
                self._state = "degraded"
                self._reconnect_count += 1
                self._last_disconnect_reason = error
                self._last_error = error
                print(f"📡 Futures WS Error on {current_url}: {exc}")
                mirror_idx += 1
                await asyncio.sleep(5)

    def get_live_context(self, *, now_ms: int | None = None) -> Dict[str, Any]:
        current_ts = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        rest_context = self._build_rest_context(now_ms=current_ts)
        if not rest_context:
            return {"available": False}

        if not self._agg_trade_rows:
            self._seed_recent_trades_once()

        agg_rows = self.recent_agg_trade_rows(now_ms=current_ts)
        force_rows = self.recent_force_orders(now_ms=current_ts)
        trade_summary = summarize_futures_agg_trades(agg_rows)
        liquidation_summary = summarize_force_orders(force_rows)
        total_trade_quote = float(trade_summary["total_quote"])
        liquidation_intensity = float(liquidation_summary["quote"]) / max(total_trade_quote, 1.0)

        ws_age_ms = 0 if self._last_ws_message_ts is None else max(0, current_ts - self._last_ws_message_ts)
        context_fresh = bool(rest_context.get("fresh", False)) and (
            self._last_ws_message_ts is None or ws_age_ms <= int(max(self.context_window_seconds * 1000, 10_000))
        )

        return {
            **rest_context,
            "fresh": context_fresh,
            "futures_delta_available": bool(trade_summary["available"]),
            "futures_delta": float(trade_summary["delta_quote"]),
            "futures_buy_quote": float(trade_summary["buy_quote"]),
            "futures_sell_quote": float(trade_summary["sell_quote"]),
            "futures_total_quote": total_trade_quote,
            "futures_trade_count": int(trade_summary["trade_count"]),
            "futures_aggression_ratio": float(trade_summary["aggression_ratio"]),
            "liquidation_context_available": self._stream_connected or bool(liquidation_summary["available"]),
            "liquidation_bias": str(liquidation_summary["bias"]),
            "liquidation_count": int(liquidation_summary["count"]),
            "liquidation_quote": float(liquidation_summary["quote"]),
            "liquidation_vol": float(liquidation_summary["quote"]),
            "liquidation_intensity": liquidation_intensity,
            "liquidation_buy_quote": float(liquidation_summary["buy_quote"]),
            "liquidation_sell_quote": float(liquidation_summary["sell_quote"]),
            "basis_state": str(rest_context.get("basis_state", "BALANCED")),
            "oi_expansion_ratio": float(rest_context.get("oi_expansion_ratio", 0.0)),
            "venue_confirmation_state": "UNCONFIRMED",
        }

    def get_health_report(self, *, now_ms: int | None = None) -> Dict[str, Any]:
        current_ts = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        ws_latency_ms = None
        if self._last_ws_message_ts:
            ws_latency_ms = current_ts - self._last_ws_message_ts

        agg_trade_age_ms = None
        if self._last_agg_trade_ts:
            agg_trade_age_ms = current_ts - self._last_agg_trade_ts

        force_order_age_ms = None
        if self._last_force_order_ts:
            force_order_age_ms = current_ts - self._last_force_order_ts

        # Stale if no message for > 15s or no trade for > window*2 (if active)
        is_stale = False
        if ws_latency_ms and ws_latency_ms > 15_000:
            is_stale = True
        elif agg_trade_age_ms and agg_trade_age_ms > (self.context_window_seconds * 2000):
            # Only mark stale if we expect trades (heuristic)
            is_stale = True

        return {
            "connected": self._stream_connected,
            "state": self._state,
            "connect_attempts": self._connect_attempts,
            "reconnect_count": self._reconnect_count,
            "message_count": self._message_count,
            "is_stale": is_stale,
            "ws_latency_ms": ws_latency_ms,
            "agg_trade_age_ms": agg_trade_age_ms,
            "force_order_age_ms": force_order_age_ms,
            "rest_cache_age_ms": current_ts - self._rest_context_ts,
            "last_message_ts": self._last_ws_message_ts,
            "idle_gap_seconds": None if ws_latency_ms is None else ws_latency_ms / 1000.0,
        }

    def health_snapshot(self, *, now_ms: int | None = None) -> CollectorHealthSnapshot:
        report = self.get_health_report(now_ms=now_ms)
        notes: list[str] = []
        if report.get("agg_trade_age_ms") is not None:
            notes.append(f"agg_trade_age={int(report['agg_trade_age_ms'])}ms")
        if report.get("force_order_age_ms") is not None:
            notes.append(f"force_order_age={int(report['force_order_age_ms'])}ms")

        return CollectorHealthSnapshot(
            venue="binance_futures",
            state=str(report.get("state", self._state)),
            connected=bool(report.get("connected", False)),
            connect_attempts=int(report.get("connect_attempts", self._connect_attempts)),
            reconnect_count=int(report.get("reconnect_count", self._reconnect_count)),
            message_count=int(report.get("message_count", self._message_count)),
            last_disconnect_reason=self._last_disconnect_reason,
            last_error=self._last_error,
            latency_ms=report.get("ws_latency_ms"),
            is_stale=bool(report.get("is_stale", False)),
            last_message_ts=report.get("last_message_ts"),
            idle_gap_seconds=report.get("idle_gap_seconds"),
            notes=tuple(notes),
        )
