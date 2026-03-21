from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

from cfte.collectors.health import CollectorErrorSurface, CollectorHealthSnapshot, CollectorState, build_error_surface

import ssl
import certifi

BYBIT_REST_MIRRORS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
    "https://api.bybit.nl",
    "https://api.bybit.be",
]
BYBIT_REST_BASE = BYBIT_REST_MIRRORS[0]
BYBIT_VENUE = "bybit"

BYBIT_WS_MIRRORS = [
    "wss://stream.bybit.com/v5/public/linear",
    "wss://stream.bytick.com/v5/public/linear",
]
BYBIT_WS_BASE = BYBIT_WS_MIRRORS[0]

COMMON_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


def build_public_topics(symbols: list[str]) -> list[str]:
    topics: list[str] = []
    for symbol in symbols:
        upper = symbol.upper().replace("-", "")
        topics.append(f"publicTrade.{upper}")
        topics.append(f"orderbook.50.{upper}") 
    return topics


def fetch_depth_snapshot(symbol: str, limit: int = 50, rest_base: str = BYBIT_REST_BASE) -> dict[str, object]:
    """Fetch L2 orderbook from Bybit V5."""
    import requests
    import random
    url = f"{rest_base}/v5/market/orderbook"
    params = {"category": "linear", "symbol": symbol.upper(), "limit": limit}
    headers = {"User-Agent": random.choice(COMMON_USER_AGENTS)}
    resp = requests.get(url, params=params, headers=headers, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise ValueError(f"Bybit API error: {data.get('retMsg')}")
    return data.get("result", {})


def try_fetch_depth_snapshot(
    symbol: str,
    limit: int = 50,
    rest_base: str = BYBIT_REST_BASE,
) -> tuple[dict[str, object] | None, str | None]:
    """Try multiple mirrors to fetch Bybit depth snapshot."""
    import requests
    mirrors = [rest_base] + [m for m in BYBIT_REST_MIRRORS if m != rest_base]
    last_error = None
    
    for mirror in mirrors:
        try:
            snapshot = fetch_depth_snapshot(symbol=symbol, limit=limit, rest_base=mirror)
            return snapshot, None
        except requests.RequestException as exc:
            last_error = exc
            print(f"⚠️ Bybit Mirror failed ({mirror}): {exc}")
            continue
            
    return None, f"Không lấy được snapshot depth Bybit cho {symbol.upper()} qua các mirrors: {last_error}"


def fetch_recent_trades(symbol: str, limit: int = 50, rest_base: str = BYBIT_REST_BASE) -> list[dict[str, object]]:
    """Fetch recent trades from Bybit V5."""
    import requests
    import random
    url = f"{rest_base}/v5/market/recent-trade"
    params = {"category": "linear", "symbol": symbol.upper(), "limit": limit}
    headers = {"User-Agent": random.choice(COMMON_USER_AGENTS)}
    resp = requests.get(url, params=params, headers=headers, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise ValueError(f"Bybit API error: {data.get('retMsg')}")
    return data.get("result", {}).get("list", [])


@dataclass(slots=True)
class BybitPublicCollector:
    topics: list[str]
    reconnect_sleep_seconds: float = 3.0
    _state: CollectorState = field(default="idle", init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)
    _connect_attempts: int = field(default=0, init=False, repr=False)
    _reconnect_count: int = field(default=0, init=False, repr=False)
    _message_count: int = field(default=0, init=False, repr=False)
    _last_message_ts: int | None = field(default=None, init=False, repr=False)
    _last_disconnect_reason: CollectorErrorSurface | None = field(default=None, init=False, repr=False)
    _last_error: CollectorErrorSurface | None = field(default=None, init=False, repr=False)

    def subscription_message(self) -> dict[str, object]:
        return {"op": "subscribe", "args": self.topics}

    def health_snapshot(self) -> CollectorHealthSnapshot:
        idle_gap_seconds = None
        is_stale = False
        if self._last_message_ts is not None:
            idle_gap_seconds = max(0.0, time.time() - (self._last_message_ts / 1000.0))
            is_stale = idle_gap_seconds > 15.0
        return CollectorHealthSnapshot(
            venue=BYBIT_VENUE,
            state=self._state,
            connected=self._connected,
            connect_attempts=self._connect_attempts,
            reconnect_count=self._reconnect_count,
            message_count=self._message_count,
            last_disconnect_reason=self._last_disconnect_reason,
            last_error=self._last_error,
            is_stale=is_stale,
            last_message_ts=self._last_message_ts,
            idle_gap_seconds=idle_gap_seconds,
        )

    def _mark_connected(self) -> None:
        self._connected = True
        self._state = "running"
        self._last_error = None

    def _record_message(self) -> None:
        self._message_count += 1
        self._last_message_ts = int(time.time() * 1000)

    def _record_failure(self, exc: Exception) -> None:
        error = build_error_surface(exc)
        self._connected = False
        self._state = "degraded"
        self._reconnect_count += 1
        self._last_disconnect_reason = error
        self._last_error = error

    async def stream_forever(self):
        mirror_idx = 0
        while True:
            try:
                import websockets
                ssl_context = ssl.create_default_context(cafile=certifi.where())

                self._connect_attempts += 1
                current_url = BYBIT_WS_MIRRORS[mirror_idx % len(BYBIT_WS_MIRRORS)]
                
                async with websockets.connect(
                    current_url, 
                    ssl=ssl_context, 
                    ping_interval=20, 
                    ping_timeout=20,
                ) as ws:
                    self._mark_connected()
                    await ws.send(json.dumps(self.subscription_message()))
                    print(f"📡 Bybit Stream Connected: {current_url}")
                    async for raw in ws:
                        self._record_message()
                        data = json.loads(raw)
                        # Handle Pong
                        if data.get("op") == "pong" or data.get("ret_msg") == "pong":
                            continue
                        yield data
            except Exception as exc:
                self._record_failure(exc)
                print(f"📡 Bybit WS Error on {current_url}: {exc}")
                mirror_idx += 1
                await asyncio.sleep(self.reconnect_sleep_seconds)
